package org.oclc.matola.ld.rowsize

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import scala.collection.mutable

/**
 * scans a table and reports on the row size (includes all columns)
 */
object RowSizeScan {
  def main(args: Array[String]): Unit = {
    val cmds = new Opts(args)
    println(s"we will scan table ${cmds.table()}")

    val spark = SparkSession.builder().appName(s"Scan ${cmds.table()}").getOrCreate()
    import spark.implicits._
    spark.conf.set("spark.dynamicAllocation.enabled", "false")

    val hBaseConf = HBaseConfiguration.create()
    val DATA = Bytes.toBytes("data")
    val DATASOURCE = Bytes.toBytes("dataSource")
    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    val cf = cmds.family.getOrElse("data").getBytes()
    val col = cmds.column.getOrElse("document").getBytes()
    scan.addFamily(cf)
    if (cmds.startKey.supplied) scan.setStartRow(cmds.startKey().getBytes())
    if (cmds.stopKey.isDefined) scan.setStopRow(cmds.stopKey().getBytes())
    if (cmds.column.isDefined) {
      val colFilter = new SingleColumnValueFilter(DATA, DATASOURCE, CompareOp.EQUAL, cmds.column.getOrElse("").getBytes)
      scan.setFilter(colFilter)
    }
    hBaseConf.set(TableInputFormat.INPUT_TABLE, cmds.table())
    hBaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val ddd = hbaseRDD.map((t: (ImmutableBytesWritable, Result)) => {
      processRow(t._1, t._2).toString
    })
    ddd.saveAsTextFile(cmds.output())
  }


  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }


  def processRow(key: ImmutableBytesWritable, result: Result): (String, Int) = {
    val DATA = Bytes.toBytes("data")
    //    val DOCUMENT = Bytes.toBytes("document")
    val id = Bytes.toString(key.get())
    //    val document: Option[Array[Byte]] = Some(result.getValue(DATA, DOCUMENT))

    var rowBytes = 0
    while (result.advance()) {
      rowBytes = rowBytes + result.current().getValueLength
    }
    (id, rowBytes)
  }

}


class Opts(args: Seq[String]) extends ScallopConf(args) {
  val table = opt[String](short = 't', required = true)
  val output = opt[String](short = 'o', required = true)
  val startKey = opt[String](short = 's')
  val stopKey = opt[String](short = 'e')
  val family = opt[String](short = 'f')
  val column = opt[String](short = 'c')

  verify()
}
