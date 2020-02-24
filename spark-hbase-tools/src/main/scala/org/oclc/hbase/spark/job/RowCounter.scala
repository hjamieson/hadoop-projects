package org.oclc.hbase.spark.job

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.spark.utils.HBaseHelper
import org.rogach.scallop.ScallopConf

/**
  * Counts rows in an HBase table.
  */
object RowCounter {


  def main(args:Array[String]): Unit ={

    val cli = new RowCounterCliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Table Reader"))

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    scan.setFilter(new FirstKeyOnlyFilter())
    if (cli.startKey.isDefined) scan.setStartRow(cli.startKey().getBytes())
    if (cli.stopKey.isDefined) scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE,cli.table())
    hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val parts = hbaseRDD.mapPartitions(iter => {
      Iterator(iter.size)
    }).repartition(1)

    parts.saveAsTextFile(cli.outputDir())

  }



}

class RowCounterCliOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val startKey = opt[String](descr = "start row", short = 's', required = false)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = false)
  val outputDir = opt[String](descr = "output directory", short='o', required = true)
  verify()
}