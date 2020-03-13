package org.oclc.hbase.spark.job

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.spark.job.serializer.BibHelper
import org.oclc.hbase.spark.utils.HBaseHelper

import scala.collection.mutable

/**
  * scans a table in HBase.
  */
object TableReader {


  def main(args: Array[String]): Unit = {

    val cli = new TableStartStopOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Table Reader"))

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("document"))
    scan.setStartRow(cli.startKey().getBytes())
    scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE, cli.table())
    hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val ddd = hbaseRDD.map(RowResult(_))
      .map(n => BibHelper(n.id, n.values()("document")))
      .map { bib =>
        s"${bib.key}|${bib.title.getOrElse("none")}|${bib.author.getOrElse("none")}|${bib.publicationDate.getOrElse("")}"
      }
      .saveAsTextFile(cli.outputDir())

  }

}

case class RowResult(tup: (ImmutableBytesWritable, Result)) {

  def id: String = Bytes.toString(tup._1.get())

  def values(): mutable.Map[String, String] = {
    val m = mutable.Map[String, String]()
    // add the rowkey:
    m += "rowkey" -> id

    val result = tup._2
    while (result.advance()) {
      val cell = result.current()
      m += (Bytes.toString(CellUtil.cloneQualifier(cell)) -> Bytes.toString(CellUtil.cloneValue(cell)))
    }
    m
  }

  override def toString: String = s"RowResult(${id.toString} => ${values.toString})"
}

