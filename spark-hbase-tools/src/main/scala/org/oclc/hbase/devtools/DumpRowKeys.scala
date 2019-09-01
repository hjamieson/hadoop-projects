package org.oclc.hbase.devtools

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.devtools.utils.HbaseHelper
import org.rogach.scallop.ScallopConf

import scala.util.Random

/**
  * Creates a dump of all the row keys in a table.  This is useful for baslining
  * processes.
  * args: table to scan
  * options: startKey, endKey
  */
object DumpRowKeys {


  def main(args:Array[String]): Unit ={

    val cli = new RowsPerRegionCliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Table Reader"))

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(1000)   // this is critical
    scan.setCacheBlocks(false)
    scan.setFilter(new FirstKeyOnlyFilter())
    if (cli.startKey.isDefined) scan.setStartRow(cli.startKey().getBytes())
    if (cli.stopKey.isDefined) scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE,cli.table())
    hBaseConf.set(TableInputFormat.SCAN, HbaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val rowkeys = hbaseRDD.map(t => Bytes.toString(t._1.get()))
    val rand = new Random()
    rowkeys
      .sample(false, .2)
      .map(p => (p, rand.nextInt(1000000)))
      .sortBy(s => s._2)
      .map(_._1)
      .saveAsTextFile(cli.outputDir())

  }

}

class DumpKeysCliOptions(args: Seq[String]) extends ScallopConf(args) {

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

