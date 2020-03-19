package org.oclc.hbase.perf.spark.job

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.perf.spark.util.{HBaseHelper, JsonMapper}

/**
  * scans the worldcat table for columns with prefix hold:
  */
object HoldingsScan {

  def main(args: Array[String]): Unit = {

    val cli = new HoldingsScanOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Holdings Scan"))
    val WORLDCAT = "Worldcat"

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    scan.addFamily("data".getBytes())
    val onlyXWC = new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("dataSource"), CompareOp.EQUAL, Bytes.toBytes("xwc"))
    onlyXWC.setFilterIfMissing(true)
    scan.setFilter(onlyXWC)
    if (cli.startKey.isDefined) scan.setStartRow(cli.startKey().getBytes())
    if (cli.stopKey.isDefined) scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE, WORLDCAT)
    hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hbaseRDD.map(convertToMapOfColumns(_))
      .map(countAllHoldings(_))
      .map(JsonMapper.toJson(_))
      .saveAsTextFile(cli.outputDir())

  }


  def convertToMapOfColumns(tup: (ImmutableBytesWritable, Result)): Map[String, String] = {

    val resp = scala.collection.mutable.Map[String, String]()
    resp("rowkey") = Bytes.toString(tup._1.get())
    val result = tup._2
    while (result.advance()) {
      val cell = result.current()
      val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
      resp(qualifier) = Bytes.toString(CellUtil.cloneValue(cell))
    }
    resp.toMap
  }

  case class Holdings(ocn: String, col: String, count: Int)

  def countAllHoldings(row: Map[String, String]): Holdings = {
    if (!row.isDefinedAt("dataSource")) {
      println(s"*** no dataSource at row ${row("rowkey")}")
      println(s"***${row.keys}***")
    }
    Holdings(row("rowkey"),
      row.getOrElse("dataSource","NaN"),
      row.filterKeys(_.startsWith("hold:")).size)
  }
}

import org.rogach.scallop.ScallopConf

class HoldingsScanOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val startKey = opt[String](descr = "start row", short = 's', required = false)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = false)
  val outputDir = opt[String](descr = "output directory", short = 'o', required = true)
  verify()
}
