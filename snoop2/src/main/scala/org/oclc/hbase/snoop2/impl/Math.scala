package org.oclc.hbase.snoop2.impl

object Math {
  def mean[T](items: Traversable[T])(implicit n: Numeric[T]) = {
    n.toDouble(items.sum) / items.size.toDouble
  }

  def variance[T](items: Traversable[T])(implicit n: Numeric[T]): Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total, item) => {
      val itemDbl = n.toDouble(item)
      val square = math.pow(itemDbl - itemMean, 2)
      total + square
    })
    sumOfSquares / count.toDouble
  }

  def stddev[T](items: Traversable[T])(implicit n: Numeric[T]) = {
    math.sqrt(variance(items))
  }
}
