package eu.stratosphere.emma.api.lara

object Utils {

  def indexed(rows: Int, cols: Int, transposed: Boolean = false): (Int, Int) => Int = {
    if (!transposed) {
      (i: Int, j: Int) => i * cols + j
    } else {
      (i: Int, j: Int) => j * rows + i
    }
  }

  def zipWithIndex[A]: A => (Int, A) = {
    var i: Int = -1
    (v: A) => {
      i += 1
      (i, v)
    }
  }
}
