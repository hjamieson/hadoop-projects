package org.oclc.hbase.snoop2

import java.util.Collections

import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.oclc.hbase.snoop2.impl.{HBaseOps, HBaseTableInfo, Observation, ObservationList, SafeList}
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

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val observations = new ObservationList
  implicit val conn = ConnectionFactory.createConnection()
  private val strTable = "Worldcat"
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
    logger.info("{}",observations.summarize(strTable))
    observations.clear
  }
  conn.close()

}
