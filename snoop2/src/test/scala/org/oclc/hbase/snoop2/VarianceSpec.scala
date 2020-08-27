package org.oclc.hbase.snoop2

import org.oclc.hbase.snoop2.impl.Math
import org.scalatest.flatspec.AnyFlatSpec

class VarianceSpec extends AnyFlatSpec {

  val list1 = (1 to 10)
  "the mean of 1 to 10" should "be 5.5" in {
    val s = (1 to 10).sum.toDouble / (1 to 10).size.toDouble
    print(s)
    assert(s == 5.5)
  }
  "the mean" should "be 5.5" in {
    assert(Math.mean(list1) == 5.5)
  }
  it should "have a variance" in {
    val s = Math.variance(1 to 10)
    println(s"var=$s")
  }
  it should "have stddev" in {
    val sd = Math.stddev(1 to 10)
    println(f"sd = ${sd}%.2f")
  }

}
