package org.oclc.hbase.perf.spark.job

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.perf.spark.util.{CdfHelper, HBaseHelper, JsonMapper}

/**
 * scans the Worldcat table that contains CDF and extracts certain fields.  The result is emitted as
 * JSON to the output.
 */
object XwcScan {

  def main(args: Array[String]): Unit = {

    val cli = new XwcScanOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("XWC Scan"))

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    scan.addFamily(Bytes.toBytes("data"))
    scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dataSource"))
    scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("document"))
    val onlyXWC = new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("dataSource"), CompareOp.EQUAL, Bytes.toBytes("xwc"))
    onlyXWC.setFilterIfMissing(true)
    scan.setFilter(onlyXWC)
    if (cli.startKey.isDefined) scan.setStartRow(cli.startKey().getBytes())
    if (cli.stopKey.isDefined) scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "Worldcat")
    hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hbaseRDD.map(convertToMapOfColumns(_))
      .map(n => CdfHelper(n("rowkey"), n("document")))
      .map(b => JsonMapper.toJson(b.dump()))
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

}


import org.rogach.scallop.ScallopConf

class XwcScanOptions(args: Seq[String]) extends ScallopConf(args) {

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
