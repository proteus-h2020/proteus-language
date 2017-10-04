package eu.proteus.lara.streaming

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

class StreamingMatrix(
    @transient ds: DataStream[Array[Double]],
    numCols: Int,
    numRows: Int = 1
  ) extends Serializable {

  self =>

  private def mapScalar(scalar: Double, f: (Double, Double) => Double): StreamingMatrix = {
    StreamingMatrix(ds.map(new MapFunction[Array[Double], Array[Double]] {
      override def map(row: Array[Double]): Array[Double] = {
        row.map(f(_, scalar))
      }
    }), numCols, numRows)
  }


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

  def +(that: Placeholder): StreamingMatrix = ???

  def -(that: Placeholder): StreamingMatrix = ???

  def *(that: Placeholder): StreamingMatrix = ???

  def /(that: Placeholder): StreamingMatrix = ???




}

object StreamingMatrix {

  def apply(
      ds: DataStream[Array[Double]],
      numCols: Int,
      numRows: Int = 1
   ) : StreamingMatrix = {
    new StreamingMatrix(ds, numCols, numRows)
  }


}
