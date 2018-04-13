package eu.proteus.lara.streaming

import org.apache.flink.streaming.api.scala._


sealed trait TraversableStreamingMatrix{
  //operator in each vertex
  var op: Char

  //checking if this node can be computed in an optimized way. Currently, by checking if it is an add operator with child-op = mult
  protected[streaming] def isOptimizable: Boolean

  //compute the expression represent from this vertex downwardly
  protected[streaming] def fuse(): StreamingMatrix

  //to datastream
  def toDataStream : DataStream[Array[Double]] = {
    fuse() toDataStream
  }

//  def +(that: TraversableStreamingMatrix): TraversableStreamingMatrix = {
//     Branch(this, that, '+')
//  }
//
//  def -(that: TraversableStreamingMatrix): TraversableStreamingMatrix = {
//     Branch(this, that, '-')
//  }
//
//  def *(that: TraversableStreamingMatrix): TraversableStreamingMatrix = {
//     Branch(this, that, '*')
//  }
//
//  def /(that: TraversableStreamingMatrix): TraversableStreamingMatrix = {
//     Branch(this, that, '/')
//  }
//
//  def %*%(that: TraversableStreamingMatrix): TraversableStreamingMatrix = {
//     Branch(this, that, '%')
//  }

}


case class Leaf(value: StreamingMatrix) extends TraversableStreamingMatrix {

  override def isOptimizable(): Boolean = false

  //identity op, consider changing it to load matrix from stream
  var op: Char = 'i'

  override def fuse(): StreamingMatrix = value

  override def toString: String = op.toString


}

case class Branch(left: TraversableStreamingMatrix, right: TraversableStreamingMatrix, operator: Char) extends TraversableStreamingMatrix{
  var op: Char = operator

  override def isOptimizable(): Boolean = {
    if (op == '+')
      if( left.op == '*' || right.op == '*') true
      else false
    else
        false
  }

   override def fuse(): StreamingMatrix = {
//    if (isOptimizable)
//      ??? //add some optimization of computation here

    //else, currently, compute normally
    val l = left.fuse()
    val r = right.fuse()

    val result = op match{
      case '+' => l.transformMatrix(r, _ +:+ _)
      case '-' => l.transformMatrix(r, _ -:- _)
      case '*' => l.transformMatrix(r, _ *:* _)
      case '/' => l.transformMatrix(r, _ /:/ _)
      case '%' => l.transformMatrix(r, _ * _)
    }
    result
  }

  override def toString: String = "(" + left.toString + ")" + op + "(" + right.toString + ")"

}

