package code.aws

/**
  * Extract bib records from Workdcat table into JSON format for export.
  */

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable


object DumpWorldcatRecords {

  def main(args: Array[String]): Unit = {
    val cli = new CliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Dump Worldcat Records"))

    val hBaseConf = HBaseConfiguration.create()

    // setup the scan
    val scan: Scan = new Scan()
    scan.setCaching(100)
    scan.setCacheBlocks(false)
    scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("document"))
    scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("priFormat"))
    scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("language"))
    scan.setStartRow(cli.startKey().getBytes())
    scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE, cli.table())
    hBaseConf.set(TableInputFormat.SCAN, HbaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val ddd = hbaseRDD.map((t: (ImmutableBytesWritable, Result)) => ResultToMap(t._1, t._2))
    val bibs = ddd.map(n => BibHelper(n.id, n.values()("document")))

    val details = bibs.map { bib => bib.toJson(JsonMapper.get)
    }
    details.saveAsTextFile(cli.outputDir())

  }

}


/**
  * Takes a Redsult and returns a map of keys to values.
  *
  * @param key
  * @param result
  */
case class ResultToMap(key: ImmutableBytesWritable, result: Result) {

  def id: String = Bytes.toString(key.get())

  def values(): mutable.Map[String, String] = {
    val m = mutable.Map[String, String]()
    // add the rowkey:
    m += "rowkey" -> id

    while (result.advance()) {
      val cell = result.current()
      m += (Bytes.toString(CellUtil.cloneQualifier(cell)) -> Bytes.toString(CellUtil.cloneValue(cell)))
    }
    m
  }

  override def toString: String = s"RowResult(${id.toString} => ${values.toString})"
}


class CliOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val startKey = opt[String](descr = "start row", short = 's', required = true)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = true)
  val outputDir = opt[String](descr = "output directory", short = 'o', required = true)
  verify()
}

