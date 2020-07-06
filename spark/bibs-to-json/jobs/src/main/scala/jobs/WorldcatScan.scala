package jobs

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Scans the Worldcat table and create an extract.
 */
object WorldcatScan extends App {

  val spark = SparkSession.builder()
    .appName("Worldcat Datalake Extract")
    .getOrCreate()

  val sc = spark.sparkContext

  val hBaseConf = HBaseConfiguration.create()

  val cli = new WorldcatScanOptions(args)

  // setup the scan
  val scan: Scan = new Scan()
  scan.setCaching(500)
  scan.setCacheBlocks(false)
  scan.addFamily(Bytes.toBytes("data"))
  scan.setStartRow(cli.startKey().getBytes())
  scan.setStopRow(cli.stopKey().getBytes())
  hBaseConf.set(TableInputFormat.INPUT_TABLE, cli.table())
  hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

  /*
  copy the CDF into a row and create a dataframe, which we will then write to HDFS.
   */

  /**
   * accepts the table row and returns the fields object
   * 1 : 100000002 = 1 rows
   */


  import spark.implicits._

  val tableDF = sc.newAPIHadoopRDD(hBaseConf,
    classOf[TableInputFormat],
    classOf[ImmutableBytesWritable],
    classOf[Result])
    .map(row => ScanHelper.rowToFields(row))
    .toDF

  tableDF.write
    .mode(SaveMode.Overwrite)
    .option("compression","snappy")
    .parquet(cli.outputDir())

}

object ScanHelper {

  case class FieldsFromRow(
                            rowkey: String,
                            document: Option[String],
                            dataSource: Option[String],
                            createDate: Option[String],
                            language: Option[String],
                            physFormat: Option[String],
                            priFormat: Option[String],
                            publisher: Option[String],
                            workId: Option[String]
                          )

  val DATA = Bytes.toBytes("data")
  val DOCUMENT = Bytes.toBytes("document")

  def rowToFields(t: (ImmutableBytesWritable, Result)): FieldsFromRow = {
    val rowkey = Bytes.toString(t._1.get())
    val result = t._2

    val doc = Option(result.getValue(DATA, DOCUMENT))
    val dataSource = Option(result.getValue(DATA, "dataSource".getBytes()))
    val createDate = Option(result.getValue(DATA, "createDate".getBytes()))
    val language = Option(result.getValue(DATA, "language".getBytes()))
    val physFormat = Option(result.getValue(DATA, "physFormat".getBytes()))
    val priFormat = Option(result.getValue(DATA, "priFormat".getBytes()))
    val publisher = Option(result.getValue(DATA, "publisher".getBytes()))
    val workId = Option(result.getValue(DATA, "workId".getBytes()))
    FieldsFromRow(rowkey,
      doc.flatMap(d => Some(new String(d))),
      dataSource.flatMap(d => Some(new String(d))),
      createDate.flatMap(d => Some(new String(d))),
      language.flatMap(d => Some(new String(d))),
      physFormat.flatMap(d => Some(new String(d))),
      priFormat.flatMap(d => Some(new String(d))),
      publisher.flatMap(d => Some(new String(d))),
      workId.flatMap(d => Some(new String(d)))
    )
  }

}


import org.rogach.scallop.ScallopConf

class WorldcatScanOptions(args: Seq[String]) extends ScallopConf(args) {

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
