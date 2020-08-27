package org.oclc.hbase.snoop2.impl

import scala.util.{Failure, Success, Try}

class ObservationList(table: String) extends SafeList[Observation] {
  /**
   * prints a summary of the results in the list
   *
   */
  def summarize: ObservationSummary = {
    val obs: List[Observation] = all()
    val times = obs.map(_.elapsedMs)
    val zeros = times.filter(_ == 0).size
    val msAvg = Math.mean(times)
    val msVar = Math.variance(times)
    val msStd = Math.stddev(times)
    val msMin = Try(times.min) match {
      case Success(v) => v
      case Failure(e) => -1
    }
    val msMax = Try(times.max) match {
      case Success(v) => v
      case _ => -1
    }
    ObservationSummary(table, System.currentTimeMillis(), obs.size, msAvg, msMin, msMax, msVar, msStd, zeros)
  }

  override def toString: String = s"ObservationList, cnt=${all().size}"

  case class ObservationSummary(
                                 table: String,
                                 timestamp: Long,
                                 numObs: Int,
                                 msAvg: Double,
                                 msMin: Long,
                                 msMax: Long,
                                 s: Double,
                                 sd: Double,
                                 zeros: Int
                               ) {
    override def toString: String = f"${table}[$timestamp]: obs(${numObs}) msAvg(${msAvg}%.2f), msMin(${msMin}), msMax(${msMax}), msVar(${s}%.1f), msStd($sd%.1f), 0s($zeros)"

  }

}
