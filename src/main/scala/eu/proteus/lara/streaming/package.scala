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

package eu.proteus.lara

import eu.proteus.lara.streaming.StreamingMatrix
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.RangePSLogicWithClose
import org.apache.flink.streaming.api.scala._
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import hu.sztaki.ilab.ps.matrix.factorization.utils.EOF
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.reflect.ClassTag


package object overrides {

  implicit def toMatrix(ds: DataStream[Array[Double]]) = new {
    def toMatrix(n: Int, m: Int) = {
      StreamingMatrix(ds, n, m)
    }
  }

  implicit def asBreeze(ds: DataStream[Array[Double]]) = new {
    def asBreezeVectors = {
      ds.map(x => (0l, BreezeDenseVector(x)))
    }
  }

  type DenseMatrix = BreezeDenseMatrix[Double]
  type DenseVector = BreezeDenseVector[Double]

  type IndexDenseMatrix = (Long, BreezeDenseMatrix[Double])
  type IndexedDenseVector = (Long, BreezeDenseVector[Double])

  def withFeedback[ModelType : ClassTag : TypeInformation](
    env: StreamExecutionEnvironment,
    input: StreamingMatrix,
    labels: StreamingMatrix,
    model: ModelType
  )(
    predictAndTrain: (DenseVector, ModelType) => (DenseVector, ModelType),
    merger: (ModelType, ModelType) => ModelType,
    update: (DenseVector, DenseVector, ModelType) => ModelType
  ): DataStream[Either[(DenseVector, DenseVector), (Int, ModelType)]] = {

    val inputStream = input.toDataStream.asBreezeVectors
    val labelStream = labels.toDataStream.asBreezeVectors
    val modelStream = env.fromCollection(List(model))

    val mixedStream = inputStream
      .connect(labelStream)
      .flatMap(new CoFlatMapFunction[IndexedDenseVector, IndexedDenseVector, Either[IndexedDenseVector, IndexedDenseVector]] {
        override def flatMap1(x: IndexedDenseVector, out: Collector[Either[IndexedDenseVector, IndexedDenseVector]]): Unit = {
          out.collect(Left(x))
        }

        override def flatMap2(y: IndexedDenseVector, out: Collector[Either[IndexedDenseVector, IndexedDenseVector]]): Unit = {
          out.collect(Right(y))
        }
      }
    )

    def rangePartitionerPS(featureCount: Int)(psParallelism: Int): WorkerToPS[ModelType] => Int = {
      val partitionSize = Math.ceil(featureCount.toDouble / psParallelism).toInt
      val partitonerFunction = (paramId: Int) => Math.abs(paramId) / partitionSize

      val paramPartitioner: WorkerToPS[ModelType] => Int = {
        case WorkerToPS(_, msg) => msg match {
          case Left(Pull(paramId)) => partitonerFunction(paramId)
          case Right(Push(paramId, _)) => partitonerFunction(paramId)
        }
      }

      paramPartitioner
    }

    val featureCount = input.numCols
    val serverLogic = new RangePSLogicWithClose[ModelType](
      featureCount,
      _ => throw new UnsupportedOperationException,
      merger
    )
    val paramPartitioner: WorkerToPS[ModelType] => Int = rangePartitionerPS(featureCount)(1)

    val wInPartition: PSToWorker[ModelType] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    val workerLogic =  WorkerLogic.addPullLimiter(
      new WorkerLogic[Either[IndexedDenseVector, IndexedDenseVector], ModelType, (DenseVector, DenseVector)] {

        val unpredictedVecs = new mutable.Queue[(Long, DenseVector)]()
        val unlabeledVecs = new mutable.HashMap[Long, DenseVector]()
        val stashedModels = new mutable.HashMap[Long, ModelType]()
        val stashedData = new mutable.Queue[(Long, DenseVector, DenseVector)]()

        override def onRecv(
          data: Either[IndexedDenseVector, IndexedDenseVector],
          ps: ParameterServerClient[ModelType, (DenseVector, DenseVector)]
        ): Unit = {
          data match {
            case Left(v) =>
              // store unlabelled point and pull
              unpredictedVecs.enqueue((v._1, v._2))
              unlabeledVecs(v._1) = v._2
            case Right(v) =>
              // we got a labelled point
              unlabeledVecs.remove(v._1) match {
                case Some(unlabeledVector) => stashedData.enqueue((v._1, unlabeledVector, v._2))
                case _ =>
              }
          }
          ps.pull(0)
        }

        override def onPullRecv(
          paramId: Int,
          currentModel: ModelType,
          ps: ParameterServerClient[ModelType, (DenseVector, DenseVector)]): Unit = {

          while (unpredictedVecs.nonEmpty) {
            val dataPoint = unpredictedVecs.dequeue()
            val (prediction, localModel) = predictAndTrain(dataPoint._2, currentModel)
            stashedModels(dataPoint._1) = localModel
            ps.output((dataPoint._2, prediction))
          }

          while (stashedData.nonEmpty) {
            val (index, dataPoint, target) = stashedData.dequeue()
            stashedModels.remove(index) match {
              case Some(localModel) => {
                val newModel = update(dataPoint, target, localModel)
                ps.push(0, newModel)
              }
              case _ =>
            }

          }
        }
      },
      10000
    )

    FlinkParameterServer.transformWithModelLoad(
      modelStream.map(x => (0, x)).broadcast
    )(
      mixedStream,
      workerLogic,
      serverLogic,
      paramPartitioner,
      wInPartition,
      inputStream.parallelism,
      1,
      20000l
    )
  }

}
