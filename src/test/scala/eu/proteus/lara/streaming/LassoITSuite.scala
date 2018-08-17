/*
 * Copyright (C) 2017 The Proteus Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.lara.streaming

import java.util.{Properties, HashMap => JHashMap, Map => JMap}

import breeze.linalg.{diag, inv, DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import breeze.numerics.{abs, sqrt}
import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.data.serializer.CoilMeasurementKryoSerializer
import eu.proteus.job.operations.data.serializer.schema.UntaggedObjectSerializationSchema
import eu.proteus.job.operations.lasso.{AggregateFlatnessValuesWindowFunction, AggregateMeasurementValuesWindowFunction}
import eu.proteus.lara.overrides._
import org.apache.flink.ml.math.{BreezeVectorConverter, Vector}
import eu.proteus.lara.streaming.flink.FlinkTestBase
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import grizzled.slf4j.Logger
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
//import eu.proteus.solma
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.io.Source

class LassoITSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {


  import LassoITSuite._

  it should "Perform Lasso for Nursery.csv dataset" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    def lineToInstance(line: String): (BreezeDenseVector[Double], BreezeDenseVector[Double]) ={
      val v = line.split(";").map(_.trim.toDouble)
      (BreezeDenseVector(v.init), BreezeDenseVector(Array(v(8))))
    }

    val featureCount = 8
    val initA = 1.0
    val initB = 0.5

    val initModel: LassoModel = (
      diag(BreezeDenseVector.fill(featureCount){initA}),
      BreezeDenseVector.fill(featureCount){initB}, 1.0
    )

    val (dataSemiShuffled, labelsCount) = shuffleData("/nursery.csv", lineToInstance)

    val mixedStream: DataStream[Either[IndexedDenseVector, IndexedDenseVector]] = env.fromCollection(dataSemiShuffled)
    val ds = withFeedback(env, mixedStream, featureCount, initModel)(predictAndTrain, merger, update)
        .addSink(new RichSinkFunction[Either[(DenseVector, DenseVector), (Int, (DenseMatrix, DenseVector, Double))]]() {
          var counter = 0
          override def invoke(value: Either[(DenseVector, DenseVector), (Int, (DenseMatrix, DenseVector, Double))], context: SinkFunction.Context[_]): Unit = {
            value match {
               case Left(_) =>
                counter += 1
                LOG.debug(s"Current count: $counter/$labelsCount")
               case Right(_) =>
            }
          }

          override def close(): Unit = {
            if (counter != labelsCount) {
              LOG.error(s"Error expected $labelsCount but got $counter")
            } else {
              LOG.info(s"all good, processed $counter")
            }
          }
        }).setParallelism(1)

    env.execute()
  }


  it should "Perform Lasso for test.csv dataset" in{
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val featureCount = 76
    val initA = 1.0
    val initB = 0.0
    val initGamma = 20.0

    val initModel: LassoModel = (
      diag(BreezeDenseVector.fill(featureCount){initA}),
      BreezeDenseVector.fill(featureCount){initB},
      initGamma
    )

    def lineToInstance(line: String): (BreezeDenseVector[Double], BreezeDenseVector[Double]) ={
      val params = line.split(",")
      val label = params(2).toDouble
      val features = params.slice(3, params.length)
      val vector = BreezeDenseVector[Double](features.map(x => x.toDouble))
      (vector, BreezeDenseVector(Array(label)))
    }

    val (dataSemiShuffled, _) = shuffleData("/tests.csv", lineToInstance)

    val mixedStream: DataStream[Either[IndexedDenseVector, IndexedDenseVector]] = env.fromCollection(dataSemiShuffled)
    val ds = withFeedback(env, mixedStream, featureCount, initModel)(predictAndTrain, merger, update)
      .print.setParallelism(1)

    env.execute()
  }


  it should "Perform Lasso with data source from Kafka " in{
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    //set memory backend as state backend
    val memoryBackendMBSize = 20
    val ONE_MEGABYTE = 1024 * 1024
    val stateBackend = new MemoryStateBackend(memoryBackendMBSize * ONE_MEGABYTE, true)
    env.setStateBackend(stateBackend)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // kafka config
    val kafkaBootstrapServer = "localhost:9092"
    val kafkaTopic = "realtime"
    val flatnessDataKafkaTopic = "realtimeFlatness"

    val cfg = env.getConfig

    // register types
    cfg.registerKryoType(classOf[CoilMeasurement])
    cfg.registerKryoType(classOf[SensorMeasurement2D])
    cfg.registerKryoType(classOf[SensorMeasurement1D])

    // register serializers
    env.addDefaultKryoSerializer(classOf[CoilMeasurement], classOf[CoilMeasurementKryoSerializer])
    env.addDefaultKryoSerializer(classOf[SensorMeasurement2D], classOf[CoilMeasurementKryoSerializer])
    env.addDefaultKryoSerializer(classOf[SensorMeasurement1D], classOf[CoilMeasurementKryoSerializer])

    env.addDefaultKryoSerializer(classOf[mutable.Queue[_]],
      classOf[com.twitter.chill.TraversableSerializer[_, mutable.Queue[_]]])

    implicit val inputTypeInfo = createTypeInformation[CoilMeasurement]
    val inputSchema = new UntaggedObjectSerializationSchema[CoilMeasurement](env.getConfig)
    val properties = new Properties
    properties.setProperty("bootstrap.servers", kafkaBootstrapServer)

    val realTimeSource: DataStream[CoilMeasurement] = env.addSource(
      new FlinkKafkaConsumer010[CoilMeasurement](
        kafkaTopic,
        inputSchema,
        properties

      )
    )

    val flatnessSource: DataStream[CoilMeasurement] = env.addSource(new FlinkKafkaConsumer010[CoilMeasurement](
      flatnessDataKafkaTopic,
      inputSchema,
      properties))

    val varId: Int = 28
    val allowedFlatnessLateness = 10000
    val allowedRealtimeLateness = 10000
        val featureCount = 76
        val initA = 1.0
        val initB = 0.0
        val initGamma = 20.0

        val initModel: LassoModel = (
          diag(BreezeDenseVector.fill(featureCount){initA}),
          BreezeDenseVector.fill(featureCount){initB},
          initGamma
        )

    val processedFlatnessStream = flatnessSource.filter(x => x.slice.head == varId)
      .keyBy(x => x.coilId)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(allowedFlatnessLateness)))
      .process(new AggregateFlatnessValuesWindowFunction())

    val processedMeasurementStream = realTimeSource.keyBy(x => getKeyByCoilAndX(x))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(allowedRealtimeLateness)))
      .process(new AggregateMeasurementValuesWindowFunction(featureCount))

    val connectedStreams = processedMeasurementStream.connect(processedFlatnessStream)

    val allEvents = connectedStreams.flatMap(new CoFlatMapFunction[LassoStreamEvent,
      LassoStreamEvent, LassoStreamEvent]() {

      override def flatMap1(value: LassoStreamEvent, out: Collector[LassoStreamEvent]): Unit = {
        out.collect(value)
      }

      override def flatMap2(value: LassoStreamEvent, out: Collector[LassoStreamEvent]): Unit = {
        out.collect(value)
      }
    })

    val mixedStream: DataStream[Either[(Long, DenseVector), (Long, DenseVector)]] = allEvents.map{
      x =>
        x match {
          case Right(label) =>
            val id = label.label
            val l = BreezeDenseVector(label.labels.map(x => x._2).toArray)
            Right((id, l))

          case Left(datapoint) =>
            val id = datapoint.pos._1
            val data = BreezeDenseVector(datapoint.data.map(x => x._2).toArray)

            Left((id, data))
        }
    }

    val ds = withFeedback(env, mixedStream, featureCount, initModel)(predictAndTrain, merger, update)
          .print.setParallelism(1)

    val str = env.getExecutionPlan
    env.execute()
  }

}

object LassoITSuite{
  val LOG = Logger(getClass)
  type LassoModel = (BreezeDenseMatrix[Double], BreezeDenseVector[Double], Double)
  def getKeyByCoilAndX(mesurement: CoilMeasurement): (Int, Double) = {
    val xCoord = mesurement match {
      case s1d: SensorMeasurement1D => s1d.x
      case s2d: SensorMeasurement2D => s2d.x
    }
    (mesurement.coilId, xCoord)
  }
  def update(x_t: BreezeDenseVector[Double], label: BreezeDenseVector[Double], model: LassoModel) : LassoModel = {
    val gamma = model._3

    val a_t: BreezeDenseMatrix[Double] = x_t.asDenseMatrix.t * x_t.asDenseMatrix

    val A_t: BreezeDenseMatrix[Double] =
      model._1 + a_t + inv(diag(BreezeDenseVector.fill(model._1.rows){sqrt(abs(gamma))}))

    val newLabel = model._2.asDenseMatrix * inv(A_t) * x_t.asDenseMatrix.t

    val l_t: BreezeDenseVector[Double] = model._2 + label.data(0) * x_t

    (A_t, l_t, gamma)
  }

  def merger(currentModel: LassoModel, model: LassoModel) : LassoModel = {
    (currentModel._1 + model._1, currentModel._2 + model._2, (currentModel._3 + model._3)/2)
  }

  def predictAndTrain(x_t: BreezeDenseVector[Double], model: LassoModel) : (BreezeDenseVector[Double], LassoModel) = {
    val A_t = model._1
    val b_t = model._2
    val y_t = b_t.toDenseMatrix * inv(A_t) * x_t
    (y_t, model)
  }


  private def shuffleData(
                   filePath: String,
                   lineToInstance: String=>(BreezeDenseVector[Double], BreezeDenseVector[Double])
                 ): (mutable.ArrayBuffer[Either[IndexedDenseVector, IndexedDenseVector]], Int) = {

    val rnd = scala.util.Random
    val bufferedSource = Source.fromInputStream(getClass.getResourceAsStream(filePath))
    var i = 0L

    val unlabelled = new mutable.HashMap[Long, BreezeDenseVector[Double]]()
    val labelled = new mutable.HashMap[Long, BreezeDenseVector[Double]]()
    val dataSemiShuffled = new mutable.ArrayBuffer[Either[IndexedDenseVector, IndexedDenseVector]]()

    for (line <- bufferedSource.getLines) {
      val (datapoint, label) = lineToInstance(line)
      unlabelled(i) = datapoint
      labelled(i) = label
      i += 1
    }

    var j = 0
    var k = 0

    val max = i
    i *= 2

    // we want to split the data points from their labels
    // invariant: the i-th data point has to appear before than the i-th label

    while (i > 0) {
      val p = rnd.nextDouble()
      if (p > 0.8 && j < max) {
        // take the j-th data point
        if (j >= k) {
          // make sure the j-th data point is after the k-th label
          dataSemiShuffled.append(Left((j, unlabelled(j))))
          j += 1
          i -= 1
        } else {
          // let's take the k-th label
          dataSemiShuffled.append(Right((k, labelled(k))))
          k += 1
          i -= 1
        }
      } else {
        if (k < j) {
          dataSemiShuffled.append(Right((k, labelled(k))))
          k += 1
          i -= 1
        } else {
          dataSemiShuffled.append(Left((j, unlabelled(j))))
          j += 1
          i -= 1
        }
      }
    }

    (dataSemiShuffled, labelled.size)
  }
}


