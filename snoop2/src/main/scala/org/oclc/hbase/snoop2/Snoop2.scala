package org.oclc.hbase.snoop2

import java.util.Collections

import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.oclc.hbase.snoop2.Snoop2.args
import org.oclc.hbase.snoop2.impl.JsonUtil.toJson
import org.oclc.hbase.snoop2.impl.{HBaseOps, HBaseTableInfo, JsonUtil, Observation, ObservationList, SafeList}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.runtime

/**
 * Snoop2 is a program that performs random GETs to HBase and emits the
 * average elapsed time for each observation.  We want to get an average, min,
 * and max for each period.
 */
object Snoop2 extends App {
  def futureGet(t: Table, key: Array[Byte], q: SafeList[Observation]): Future[Observation] = Future {
    val obs = HBaseOps.doGetRow(t, key)
    q += obs
    obs
  }

  /*
  args: 0 = table name
        1 = json|text
   */

  private val strTable = if (args.length > 0) args(0) else "Worldcat"
  private val jsonOutput = if (args.length > 1 && args(1).toLowerCase == "json") true else false

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val observations = new ObservationList(strTable)
  implicit val conn = ConnectionFactory.createConnection()
  val hbaseTableInfo = HBaseTableInfo(strTable)
  val table = conn.getTable(hbaseTableInfo.tableName)

  val closer = new Thread {
    override def run(): Unit = {
      println("in shutdown...")
      conn.close()
    }
  }
  runtime.addShutdownHook(closer)

  // do forever
  while (true) {
    val work = HBaseOps.getStartKeys(strTable).map(futureGet(table, _, observations))
    Thread.sleep(15000)
    if (jsonOutput) println(toJson(observations.summarize))
    else logger.info(observations.summarize.toString)
    observations.clear
  }
  conn.close()

}
