package org.oclc.hbase.snoop2.impl

import org.scalatest.flatspec.AnyFlatSpec

class ObservationSpec extends AnyFlatSpec {
  val obs = new ObservationList("hugh")
  val dummy = Array[Byte]()
  obs += Observation("hugh", dummy, 0, 1)
  obs += Observation("hugh", dummy, 0, 5)
  obs += Observation("hugh", dummy, 0, 10)
  obs += Observation("hugh", dummy, 0, 0)

  private val summary: obs.ObservationSummary = obs.summarize
  println(summary)
  val l1 = Seq(1,5,10, 0)
  "1, 5, 10, 0" should "have a mean of 4" in {
    val computedMean = Math.mean(l1)
    println(s"Math.mean()= $computedMean")
    assert(computedMean == 4.0 )
  }
  "a summary" should "have a avg=4" in {
    assert(summary.msAvg == 4.0)
  }

  it should "have 1 zero obs" in {
    assert(summary.zeros == 1)
  }
}
