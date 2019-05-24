package code.aws

/**
  * Extract bib records from Workdcat table into JSON format for export.  This
  * version repartitions the data before conversion.
  */

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable


object DumpWorldcatRecordsWithRepartition {

  def main(args: Array[String]): Unit = {
    val cli = new CliOptionsR(args)
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

    val ddd = hbaseRDD.map((t: (ImmutableBytesWritable, Result)) => ResultToMapR(t._1, t._2)).repartition(cli.repartition.getOrElse(2))

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
case class ResultToMapR(key: ImmutableBytesWritable, result: Result) {

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


class CliOptionsR(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val startKey = opt[String](descr = "start row", short = 's', required = true)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = true)
  val outputDir = opt[String](descr = "output directory", short = 'o', required = true)
  val repartition = opt[Int](descr = "repartition", short ='r', required = false)
  verify()
}

