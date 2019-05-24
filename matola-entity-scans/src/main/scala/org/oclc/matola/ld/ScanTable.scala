package org.oclc.matola.ld

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import scala.collection.mutable

object ScanTable {
  def main(args: Array[String]): Unit = {
    val cmds = new Opts(args)
    println(s"we will scan table ${cmds.table()}")

    val spark = SparkSession.builder().appName(s"Scan ${cmds.table()}").getOrCreate()
    import spark.implicits._

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    val cf = cmds.family.getOrElse("data").getBytes()
    val col = cmds.column.getOrElse("document").getBytes()
    val cols = cmds.column.getOrElse("document")
    scan.addFamily(cf)
    if (cmds.startKey.supplied) scan.setStartRow(cmds.startKey().getBytes())
    if (cmds.stopKey.isDefined) scan.setStopRow(cmds.stopKey().getBytes())
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


  def processRow(key: ImmutableBytesWritable, result: Result): RowData = {
    val DATA = Bytes.toBytes("data")
    val DOCUMENT = Bytes.toBytes("document")
    val id = Bytes.toString(key.get())
    val document: Option[Array[Byte]] = Some(result.getValue(DATA, DOCUMENT))

    var rowBytes = 0
    while (result.advance()) {
      rowBytes = rowBytes + result.current().getValueLength
    }
    try {
      RowData(id, document.getOrElse(Array()).length, rowBytes, result.size())
    } catch {
      case npe:NullPointerException => {
        println(s"NPE for row $id")
        RowData(id, -1, -1, -1)
      }
    }
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
