package eu.proteus.lara.streaming

import MatrixOp.{MatrixOp, _}
import org.apache.flink.streaming.api.scala._

sealed trait TraversableStreamingMatrix{
  //operator in each vertex
  var op: MatrixOp

  //checking if this node can be computed in an optimized way. Currently, by checking if it is an add operator with child-op = mult
  protected[streaming] def isOptimizable: Boolean

  //compute the expression represent from this vertex downwardly
  protected[streaming] def fuse(): StreamingMatrix

  //to datastream
  def toDataStream : DataStream[Array[Double]] = {
    fuse() toDataStream
  }

}


case class Leaf(value: StreamingMatrix) extends TraversableStreamingMatrix {

  override def isOptimizable(): Boolean = false

  //identity op, consider changing it to load matrix from stream
  var op: MatrixOp = load

  override def fuse(): StreamingMatrix = value

  override def toString: String = op.toString


}

case class Branch(left: TraversableStreamingMatrix, right: TraversableStreamingMatrix, operator: MatrixOp) extends TraversableStreamingMatrix{
  var op: MatrixOp = operator

  override def isOptimizable: Boolean = {
    if (op == MatrixOp.+)
      if( left.op == * || right.op == *) true
      else false
    else
        false
  }

   override def fuse(): StreamingMatrix = {
    val l = left.fuse()
    val r = right.fuse()

    val result = op match{
      case + => l.transformMatrix(r, _ +:+ _)
      case - => l.transformMatrix(r, _ -:- _)
      case * => l.transformMatrix(r, _ *:* _)
      case / => l.transformMatrix(r, _ /:/ _)
      case %*% => l.transformMatrix(r, _ * _)
    }
    result
  }

  override def toString: String = "(" + left.toString + ")" + op + "(" + right.toString + ")"

}

