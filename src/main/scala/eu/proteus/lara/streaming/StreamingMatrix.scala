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

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import eu.proteus.lara.streaming.MatrixOp._
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class StreamingMatrix(
    @transient private[lara] val ds: DataStream[Array[Double]],
    private[lara] val numCols: Int,
    private[lara] val numRows: Int = 1
  ) extends Serializable {

  self =>

  private[lara] var tree: TraversableStreamingMatrix = Leaf(this)


  private[lara] def mapScalar(scalar: Double, f: (Double, Double) => Double): StreamingMatrix = {
    StreamingMatrix(ds.map(new MapFunction[Array[Double], Array[Double]] {
      override def map(row: Array[Double]): Array[Double] = {
        row.map(f(_, scalar))
      }
    }), numCols, numRows)
  }

  private[lara] def transformMatrix(
    that: StreamingMatrix,
    f: (BreezeDenseMatrix[Double], BreezeDenseMatrix[Double]) => BreezeDenseMatrix[Double])
  : StreamingMatrix = {
    // TODO add asserts
    StreamingMatrix(
      ds
        .countWindowAll(numRows)
        .apply(new AllWindowFunction[Array[Double], BreezeDenseMatrix[Double], GlobalWindow] {
          override def apply(
              window: GlobalWindow,
              input: Iterable[Array[Double]],
              out: Collector[BreezeDenseMatrix[Double]])
          : Unit = {
            val data = new Array[Double](numRows * numCols)
            val ret = BreezeDenseMatrix.create[Double](numRows, numCols, data)
            for ((row, i) <- input.view.zipWithIndex) {
              ret(i, ::) := BreezeDenseVector.create[Double](row, 0, 1, numCols).t
            }
            out.collect(ret)
          }
        })
        .connect(
            that.ds
              .countWindowAll(that.numRows)
              .apply(new AllWindowFunction[Array[Double], BreezeDenseMatrix[Double], GlobalWindow] {
                override def apply(
                    window: GlobalWindow,
                    input: Iterable[Array[Double]],
                    out: Collector[BreezeDenseMatrix[Double]])
                : Unit = {
                  val data = new Array[Double](that.numRows * that.numCols)
                  val ret = BreezeDenseMatrix.create[Double](that.numRows, that.numCols, data)
                  for ((row, i) <- input.view.zipWithIndex) {
                    ret(i, ::) := BreezeDenseVector.create[Double](row, 0, 1, numCols).t
                  }
                  out.collect(ret)
                }
              })
        )
        .flatMap(new RichCoFlatMapFunction[BreezeDenseMatrix[Double], BreezeDenseMatrix[Double], Array[Double]] {

          // TODO this should be eventually implemented as operator state
          @transient var q1: mutable.Queue[BreezeDenseMatrix[Double]] = _
          @transient var q2: mutable.Queue[BreezeDenseMatrix[Double]] = _

          override def open(parameters: Configuration) = {
            super.open(parameters)

            q1 = new mutable.Queue[BreezeDenseMatrix[Double]]()
            q2 = new mutable.Queue[BreezeDenseMatrix[Double]]()

          }

          override def flatMap1(in1: BreezeDenseMatrix[Double], collector: Collector[Array[Double]]) = {

            if (q2.isEmpty) {
              q1.enqueue(in1)
            } else {
              val m2 = q2.dequeue()
              val res = f(in1, m2)
              val dat = res.t.toArray
              for (i <- 0 until res.rows)
                collector.collect(dat.slice(numCols*i, numCols*(i +1)))
            }

          }

          override def flatMap2(in2: BreezeDenseMatrix[Double], collector: Collector[Array[Double]]) = {

            if (q1.isEmpty) {
              q2.enqueue(in2)
            } else {
              val m1 = q1.dequeue()
              val res = f(m1, in2)
              val dat = res.t.toArray
              for (i <- 0 until res.rows)
                collector.collect(dat.slice(numCols*i, numCols*(i +1)))
            }

          }
        }), numCols, numRows)
  }

  private[lara] def transformMatrixOptimized(
                                              sm2: StreamingMatrix,
                                              sm3: StreamingMatrix,
                                              f12: (BreezeDenseMatrix[Double], BreezeDenseMatrix[Double]) => BreezeDenseMatrix[Double],
                                              f123: (BreezeDenseMatrix[Double], BreezeDenseMatrix[Double]) => BreezeDenseMatrix[Double])
  : StreamingMatrix = {
    // TODO add asserts
    StreamingMatrix(
      ds
        .countWindowAll(numRows)
        .apply(new AllWindowFunction[Array[Double], (BreezeDenseMatrix[Double], Int), GlobalWindow] {
          override def apply(
                              window: GlobalWindow,
                              input: Iterable[Array[Double]],
                              out: Collector[(BreezeDenseMatrix[Double], Int)])
          : Unit = {
            val data = new Array[Double](numRows * numCols)
            val ret = BreezeDenseMatrix.create[Double](numRows, numCols, data)
            for ((row, i) <- input.view.zipWithIndex) {
              ret(i, ::) := BreezeDenseVector.create[Double](row, 0, 1, numCols).t
            }
            out.collect((ret, tree.id))
          }
        })
        .union(
          sm2.ds
            .countWindowAll(sm2.numRows)
            .apply(new AllWindowFunction[Array[Double], (BreezeDenseMatrix[Double], Int), GlobalWindow] {
              override def apply(
                                  window: GlobalWindow,
                                  input: Iterable[Array[Double]],
                                  out: Collector[(BreezeDenseMatrix[Double], Int)])
              : Unit = {
                val data = new Array[Double](sm2.numRows * sm2.numCols)
                val ret = BreezeDenseMatrix.create[Double](sm2.numRows, sm2.numCols, data)
                for ((row, i) <- input.view.zipWithIndex) {
                  ret(i, ::) := BreezeDenseVector.create[Double](row, 0, 1, numCols).t
                }
                out.collect((ret, sm2.tree.id))
              }
            })
        )
        .connect(
              sm3.ds
                .countWindowAll(sm3.numRows)
                .apply(new AllWindowFunction[Array[Double], (BreezeDenseMatrix[Double], Int), GlobalWindow] {
                  override def apply(
                                      window: GlobalWindow,
                                      input: Iterable[Array[Double]],
                                      out: Collector[(BreezeDenseMatrix[Double], Int)])
                  : Unit = {
                    val data = new Array[Double](sm3.numRows * sm3.numCols)
                    val ret = BreezeDenseMatrix.create[Double](sm3.numRows, sm3.numCols, data)
                    for ((row, i) <- input.view.zipWithIndex) {
                      ret(i, ::) := BreezeDenseVector.create[Double](row, 0, 1, numCols).t
                    }
                    out.collect((ret, sm3.tree.id))
                  }
                })
            )
        .flatMap(new RichCoFlatMapFunction[(BreezeDenseMatrix[Double], Int), (BreezeDenseMatrix[Double], Int), Array[Double]] {

          @transient var q1: mutable.Queue[(BreezeDenseMatrix[Double], Int)] = _
          @transient var q2: mutable.Queue[(BreezeDenseMatrix[Double], Int)] = _
          @transient var q12: mutable.Queue[BreezeDenseMatrix[Double]] = _
          @transient var q3: mutable.Queue[(BreezeDenseMatrix[Double], Int)] = _

          override def open(parameters: Configuration) = {
            super.open(parameters)

            q1 = new mutable.Queue[(BreezeDenseMatrix[Double], Int)]()
            q2 = new mutable.Queue[(BreezeDenseMatrix[Double], Int)]()
            q12 = new mutable.Queue[BreezeDenseMatrix[Double]]()
            q3 = new mutable.Queue[(BreezeDenseMatrix[Double], Int)]()

          }
          override def flatMap2(in: (BreezeDenseMatrix[Double], Int), collector: Collector[Array[Double]]): Unit = {
            if (q12.isEmpty){
                q3.enqueue(in)
            } else {
              val m12 = q12.dequeue()
              val res = f123(m12, in._1)
              val dat = res.t.toArray
              for (i <- 0 until res.rows)
                collector.collect(dat.slice(numCols*i, numCols*(i +1)))
            }

          }

          override def flatMap1(in: (BreezeDenseMatrix[Double], Int), collector: Collector[Array[Double]]): Unit = {
              if (in._2 == sm2.tree.id)
                q2.enqueue(in)
              else
                q1.enqueue(in)

              if (q1.nonEmpty && q2.nonEmpty){
                val m1 = q1.dequeue()
                val m2 = q2.dequeue()
                val m12 = f12(m1._1, m2._1)

                if (q3.nonEmpty) {
                  val res = f123(m12, q3.dequeue()._1)
                  val dat = res.t.toArray
                  for (i <- 0 until res.rows)
                    collector.collect(dat.slice(numCols * i, numCols * (i + 1)))
                }
                else
                  q12.enqueue(m12)
              }
          }
        }), numCols, numRows)
       
  }
  // scalastyle:off method.name

  //////////////////////////////////////////
  // pointwise M o scalar
  //////////////////////////////////////////

  def +(that: Double): StreamingMatrix = mapScalar(that, _ + _)
  def -(that: Double): StreamingMatrix = mapScalar(that, _ - _)
  def *(that: Double): StreamingMatrix = mapScalar(that, _ * _)
  def /(that: Double): StreamingMatrix = mapScalar(that, _ / _)

  //////////////////////////////////////////
  // pointwise M o placeholder
  //////////////////////////////////////////


  def +(that: StreamingMatrix): StreamingMatrix = {
    computeMatrixExpression(that, MatrixOp.+)
  }

  def -(that: StreamingMatrix): StreamingMatrix = {
    computeMatrixExpression(that, MatrixOp.-)
  }

  def *(that: StreamingMatrix): StreamingMatrix = {
    computeMatrixExpression(that, MatrixOp.*)
  }

  def /(that: StreamingMatrix): StreamingMatrix = {
    computeMatrixExpression(that, MatrixOp./)
  }

  def %*%(that: StreamingMatrix): StreamingMatrix = {
    computeMatrixExpression(that, MatrixOp.%*%)
  }

  private def computeMatrixExpression(that: StreamingMatrix, op: MatrixOp): StreamingMatrix = {
    val res = StreamingMatrix(ds=null, numCols, numRows)
    res.tree = Branch(this.tree, that.tree, op)
    res
  }

  // scalastyle:on method.name

  //////////////////////////////////////////
  // extract data stream
  //////////////////////////////////////////

  def toDataStream = {
    if (ds == null) {
      val sm = tree.fuse()
      tree = Leaf(sm)
      sm.ds
    }
    else
      ds
  }

}

object StreamingMatrix {
  
  def apply(
      ds: DataStream[Array[Double]],
      numCols: Int,
      numRows: Int
   ) : StreamingMatrix = {
    new StreamingMatrix(ds, numCols, numRows)
  }

  def apply(
    matrix: StreamingMatrix,
    numCols: Int,
    numRows: Int
  ) : StreamingMatrix = {
    new StreamingMatrix(matrix.ds, numCols, numRows)
  }

  def apply(
    matrix: StreamingMatrix,
    numRows: Int
  ) : StreamingMatrix = {
    new StreamingMatrix(matrix.ds, matrix.numCols, numRows)
  }

}
