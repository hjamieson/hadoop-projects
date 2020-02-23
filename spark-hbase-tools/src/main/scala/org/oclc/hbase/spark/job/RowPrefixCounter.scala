package org.oclc.hbase.spark.job

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.spark.utils.HBaseHelper
import org.rogach.scallop.ScallopConf

/**
  * Counts rows in an HBase table with the given prefix.
  * args: table, prefix, output
  */
object RowPrefixCounter {


  def main(args:Array[String]): Unit ={

    val cli = new RpcCliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Table Reader"))

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("org.oclc.hbase.tools.hbase.client.scanner.timeout.period","240000")
    hBaseConf.set("org.oclc.hbase.tools.hbase.rpc.timeout","240000")

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)

    val filter = new FilterList()
//    filter.addFilter(new KeyOnlyFilter())
//    filter.addFilter(new FirstKeyOnlyFilter())
    filter.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(cli.prefix().getBytes())))
    scan.setFilter(filter)
    hBaseConf.set(TableInputFormat.INPUT_TABLE,cli.table())
    hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val matches = hbaseRDD.count()

    println(s"$matches found with prefix ${cli.prefix()}")

  }



}

class RpcCliOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val prefix = opt[String](descr = "row prefix", short = 'p', required = true)
  val outputDir = opt[String](descr = "output directory", short='o', required = true)
  verify()
}


