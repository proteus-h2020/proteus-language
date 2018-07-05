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

import MatrixOp.{MatrixOp, _}
import breeze.linalg.{DenseMatrix, NumericOps}
import org.apache.flink.streaming.api.scala._

sealed trait TraversableStreamingMatrix{

  val id: Int

  //operator in each vertex
  var op: MatrixOp

  //checking if this node can be computed in an optimized way
  protected[streaming] def isOptimizable: Boolean

  //compute the expression represent from this vertex downwardly
  protected[streaming] def fuse(): StreamingMatrix

  //to datastream
  def toDataStream : DataStream[Array[Double]] = {
    fuse() toDataStream
  }

}

object TraversableStreamingMatrix{
  var treeID: Int = 0
}


case class Leaf(value: StreamingMatrix) extends TraversableStreamingMatrix {
  final val id: Int = {
    TraversableStreamingMatrix.treeID+=1
    TraversableStreamingMatrix.treeID
  }

  override def isOptimizable(): Boolean = false

  //identity op, consider changing it to load matrix from stream
  var op: MatrixOp = load

  override def fuse(): StreamingMatrix = value

  override def toString: String = op.toString


}

case class Branch(
                   left: TraversableStreamingMatrix,
                   right: TraversableStreamingMatrix,
                   operator: MatrixOp) extends TraversableStreamingMatrix{

  final val id: Int = {
    TraversableStreamingMatrix.treeID+=1
    TraversableStreamingMatrix.treeID
  }

  var op: MatrixOp = operator

  override def isOptimizable: Boolean = {
      left.op != load || right.op != load
  }

   override def fuse(): StreamingMatrix = {
     if (isOptimizable){
      //left-optimizable

      if (left.op != load){
        val leftBranch = left.asInstanceOf[Branch]
        val leftBranch_left = leftBranch.left.fuse()
        val leftBranch_right = leftBranch.right.fuse()

        leftBranch_left.transformMatrixOptimized(leftBranch_right, right.fuse(), convert(left.op)(), convert(op)())
      }
      //else the expression is right-optimizable
      else{
        val rightBranch = right.asInstanceOf[Branch]
        val rightBranch_left = rightBranch.left.fuse()
        val rightBranch_right = rightBranch.right.fuse()
        rightBranch_left.transformMatrixOptimized(rightBranch_right, left.fuse(), convert(right.op)(), convert(op)())
      }
    }
    else
     {
      val l = left.fuse()
      val r = right.fuse()

      val result = op match {
        case + => l.transformMatrix(r, _ +:+ _)
        case - => l.transformMatrix(r, _ -:- _)
        case * => l.transformMatrix(r, _ *:* _)
        case / => l.transformMatrix(r, _ /:/ _)
        case %*% => l.transformMatrix(r, _ * _)
      }
      result
    }
  }

  override def toString: String = "(" + left.toString + ")" + op + "(" + right.toString + ")"


  private def convert(matrixOp: MatrixOp)(): (DenseMatrix[Double], DenseMatrix[Double]) => DenseMatrix[Double] = {
    matrixOp match {
      case MatrixOp. + => _ +:+ _
      case MatrixOp.- => _ -:- _
      case MatrixOp.* => _ *:* _
      case MatrixOp./ => _ /:/ _
      case MatrixOp.%*% => _ * _
    }
  }
}

