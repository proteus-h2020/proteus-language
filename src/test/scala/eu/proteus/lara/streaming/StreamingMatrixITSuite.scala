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

import eu.proteus.lara.overrides._
import eu.proteus.lara.streaming.flink.FlinkTestBase
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.contrib.streaming.scala.utils._
import breeze.linalg.{sum, DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}

class StreamingMatrixITSuite
    extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "Streaming Matrix"

  it should "perform scalar add operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source = env.fromCollection(Array(Array(1.0, 2.2, 3.3), Array(1.0, 2.2, 3.3)))

    val matrix = StreamingMatrix(source, 3, 1)

    val res = matrix + 2.0

    val streamIt = res.toDataStream.collect()

    while (streamIt.hasNext) {
      val elem = streamIt.next
      elem(0) should be (3.0)
      elem(1) should be (4.2)
      elem(2) should be (5.3)
    }

  }

  it should "perform scalar sub operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source = env.fromCollection(Array(Array(1.0, 2.2, 3.3), Array(1.0, 2.2, 3.3)))

    val matrix = StreamingMatrix(source, 3, 1)

    val res = matrix - 2.0

    val streamIt = res.toDataStream.collect()

    while (streamIt.hasNext) {
      val elem = streamIt.next
      elem(0) should be (1.0 - 2.0)
      elem(1) should be (2.2 - 2.0)
      elem(2) should be (3.3 - 2.0)
    }

  }

  it should "perform scalar multiplication operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source = env.fromCollection(Array(Array(1.0, 2.2, 3.3), Array(1.0, 2.2, 3.3)))

    val matrix = StreamingMatrix(source, 3, 1)

    val res = matrix * 2.0

    val streamIt = res.toDataStream.collect()

    while (streamIt.hasNext) {
      val elem = streamIt.next
      elem(0) should be (2.0)
      elem(1) should be (4.4)
      elem(2) should be (6.6)
    }

  }

  it should "perform scalar div operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source = env.fromCollection(Array(Array(1.0, 2.2, 3.3), Array(1.0, 2.2, 3.3)))

    val matrix = source.toMatrix(3, 1)

    val res = matrix / 2.0

    val streamIt = res.toDataStream.collect()

    while (streamIt.hasNext) {
      val elem = streamIt.next
      elem(0) should be (0.5)
      elem(1) should be (1.1)
      elem(2) should be (3.3 / 2.0)
    }

  }


  it should "perform scalar matrix-matrix add operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source1 = env.fromCollection(Array(Array(1.0, 2.2, 3.3), Array(1.0, 2.2, 3.3)))
    val source2 = env.fromCollection(Array(Array(1.0, 2.2, 3.3), Array(1.0, 2.2, 3.3)))

    val matrix1 = StreamingMatrix(source1, 3, 2)
    val matrix2 = StreamingMatrix(source2, 3, 2)

    val res = matrix1 - matrix2

    val streamIt = res.toDataStream.collect()

    while (streamIt.hasNext) {
      val data = streamIt.next
      val m = BreezeDenseMatrix.create[Double](3, 2, data)
      sum(m) should be (0.0)
    }

  }

  it should "perform scalar matrix-matrix mult operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source1 = env.fromCollection(M1)
    val source2 = env.fromCollection(M2)

    val matrix1 = StreamingMatrix(source1, 5, 5)
    val matrix2 = source2.toMatrix(5, 5) // alternative  to StreamingMatrix(source2, 5, 5)

    val res = matrix1 %*% matrix2

    val tmp1 = BreezeDenseMatrix.zeros[Double](5, 5)
    val tmp2 = BreezeDenseMatrix.zeros[Double](5, 5)

    for (x <- 0 until 5) {
      tmp1(x, ::) := BreezeDenseVector.create[Double](M1(x), 0, 0, 5).t
      tmp2(x, ::) := BreezeDenseVector.create[Double](M2(x), 0, 0, 5).t
    }

    val M3 = tmp1 * tmp2

    val streamIt = res.toDataStream.collect()

    val eps = 1E-5
    while (streamIt.hasNext) {
      val data = streamIt.next()
      val m = BreezeDenseMatrix.create[Double](5, 5, data)
      val err: BreezeDenseMatrix[Double] = m - M3
      err foreachValue {
        (x) => x should be (0.0 +- eps)
      }
    }

  }

  it should "perform scalar matrix-matrix op-wise mult operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source1 = env.fromCollection(M1)
    val source2 = env.fromCollection(M2)

    val matrix1 = StreamingMatrix(source1, 5, 5)
    val matrix2 = source2.toMatrix(5, 5) // alternative  to StreamingMatrix(source2, 5, 5)

    val res = matrix1 * matrix2

    val tmp1 = BreezeDenseMatrix.zeros[Double](5, 5)
    val tmp2 = BreezeDenseMatrix.zeros[Double](5, 5)

    for (x <- 0 until 5) {
      tmp1(x, ::) := BreezeDenseVector.create[Double](M1(x), 0, 0, 5).t
      tmp2(x, ::) := BreezeDenseVector.create[Double](M2(x), 0, 0, 5).t
    }

    val M3 = tmp1 *:* tmp2

    val streamIt = res.toDataStream.collect()

    val eps = 1E-5
    while (streamIt.hasNext) {
      val data = streamIt.next()
      val m = BreezeDenseMatrix.create[Double](5, 5, data)
      val err: BreezeDenseMatrix[Double] = m - M3
      err foreachValue {
        (x) => x should be (0.0 +- eps)
      }
    }

  }

  it should "perform scalar matrix-matrix op-wise add operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source1 = env.fromCollection(M1)
    val source2 = env.fromCollection(M2)

    val matrix1 = StreamingMatrix(source1, 5, 5)
    val matrix2 = source2.toMatrix(5, 5) // alternative  to StreamingMatrix(source2, 5, 5)

    val res = matrix1 + matrix2

    val tmp1 = BreezeDenseMatrix.zeros[Double](5, 5)
    val tmp2 = BreezeDenseMatrix.zeros[Double](5, 5)

    for (x <- 0 until 5) {
      tmp1(x, ::) := BreezeDenseVector.create[Double](M1(x), 0, 0, 5).t
      tmp2(x, ::) := BreezeDenseVector.create[Double](M2(x), 0, 0, 5).t
    }

    val M3 = tmp1 +:+ tmp2

    val streamIt = res.toDataStream.collect()

    val eps = 1E-5
    while (streamIt.hasNext) {
      val data = streamIt.next()
      val m = BreezeDenseMatrix.create[Double](5, 5, data)
      val err: BreezeDenseMatrix[Double] = m - M3
      err foreachValue {
        (x) => x should be (0.0 +- eps)
      }
    }

  }

  it should "perform scalar matrix-matrix op-wise sub operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source1 = env.fromCollection(M1)
    val source2 = env.fromCollection(M2)

    val matrix1 = StreamingMatrix(source1, 5, 5)
    val matrix2 = source2.toMatrix(5, 5) // alternative  to StreamingMatrix(source2, 5, 5)

    val res = matrix1 - matrix2

    val tmp1 = BreezeDenseMatrix.zeros[Double](5, 5)
    val tmp2 = BreezeDenseMatrix.zeros[Double](5, 5)

    for (x <- 0 until 5) {
      tmp1(x, ::) := BreezeDenseVector.create[Double](M1(x), 0, 0, 5).t
      tmp2(x, ::) := BreezeDenseVector.create[Double](M2(x), 0, 0, 5).t
    }

    val M3 = tmp1 -:- tmp2

    val streamIt = res.toDataStream.collect()

    val eps = 1E-5
    while (streamIt.hasNext) {
      val data = streamIt.next()
      val m = BreezeDenseMatrix.create[Double](5, 5, data)
      val err: BreezeDenseMatrix[Double] = m - M3
      err foreachValue {
        (x) => x should be (0.0 +- eps)
      }
    }

  }

  it should "perform scalar matrix-matrix op-wise div operation" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val source1 = env.fromCollection(M1)
    val source2 = env.fromCollection(M2)

    val matrix1 = StreamingMatrix(source1, 5, 5)
    val matrix2 = source2.toMatrix(5, 5) // alternative  to StreamingMatrix(source2, 5, 5)

    val res = matrix1 / matrix2

    val tmp1 = BreezeDenseMatrix.zeros[Double](5, 5)
    val tmp2 = BreezeDenseMatrix.zeros[Double](5, 5)

    for (x <- 0 until 5) {
      tmp1(x, ::) := BreezeDenseVector.create[Double](M1(x), 0, 0, 5).t
      tmp2(x, ::) := BreezeDenseVector.create[Double](M2(x), 0, 0, 5).t
    }

    val M3 = tmp1 /:/ tmp2

    val streamIt = res.toDataStream.collect()

    val eps = 1E-5
    while (streamIt.hasNext) {
      val data = streamIt.next()
      val m = BreezeDenseMatrix.create[Double](5, 5, data)
      val err: BreezeDenseMatrix[Double] = m - M3
      err foreachValue {
        (x) => x should be (0.0 +- eps)
      }
    }

  }

  val M1 = Array(
    Array(12.570662, 0.6784805, 7.596820, 1.323459, 13.096651),
    Array(31.604796, 19.8837866, 7.703606, 13.824693, 8.535539),
    Array(28.996163, 0.4640472, 36.576710, 4.176056, 2.786647),
    Array(10.050648, 20.0534378, 17.620477, 5.719599, 2.052746),
    Array(6.513772, 12.5083260, 24.409236, 12.156980, 2.199044)
  )


  val M2 = Array(
    Array(3.523330, 2.8025222, 37.825223, 9.013307, 14.841940),
    Array(1.140146, 8.7096280, 7.363931, 3.521247, 6.405469),
    Array(27.658558, 0.2029765, 5.253178, 8.035369, 37.324684),
    Array(1.007423, 12.0026804, 18.438317, 6.979779, 5.189299),
    Array(3.024786, 21.8026119, 11.651798, 34.974341, 5.005692)
  )


}
