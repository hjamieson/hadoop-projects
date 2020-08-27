package org.oclc.hbase.snoop2.impl

class ObservationList extends SafeList[Observation] {
  /**
   * prints a summary of the results in the list
   * @param table
   */
  def summarize(table: String): String = {
    val obs:List[Observation] = all()
    val times = obs.map(_.elapsedMs)
    val totalElapsed: Long = times.reduce(_+_)
    val avg = (totalElapsed/obs.size)
    s"$table: obs(${obs.size}) msAvg($avg), msMin(${times.min}), msMax(${times.max})"
  }

}
