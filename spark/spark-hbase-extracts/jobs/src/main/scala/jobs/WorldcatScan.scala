package jobs

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueExcludeFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Scans the Worldcat table and create an extract.
 */
object WorldcatScan extends App {

  val spark = SparkSession.builder()
    .appName("Worldcat Datalake Extract No XWC")
    .getOrCreate()

  val sc = spark.sparkContext

  val hBaseConf = HBaseConfiguration.create()

  val cli = new WorldcatScanOptions(args)

  // setup the scan
  val scan: Scan = new Scan()
  scan.setCaching(500)
  scan.setCacheBlocks(false)
  scan.addFamily(Bytes.toBytes("data"))
  if (cli.startKey.isDefined) scan.setStartRow(cli.startKey().getBytes())
  if (cli.stopKey.isDefined) scan.setStopRow(cli.stopKey().getBytes())
  private val filterXWC = new SingleColumnValueFilter("data".getBytes(), "dataSource".getBytes(), CompareOp.NOT_EQUAL, "xwc".getBytes())
  private val filterNull = new SingleColumnValueFilter("data".getBytes(), "dataSource".getBytes(), CompareOp.NOT_EQUAL, "".getBytes())
  filterNull.setFilterIfMissing(true)
  scan.setFilter(new FilterList(filterNull, filterXWC))
  hBaseConf.set(TableInputFormat.INPUT_TABLE, cli.table.getOrElse("Worldcat"))
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
    .option("compression", "snappy")
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


  def rowToFields(t: (ImmutableBytesWritable, Result)): FieldsFromRow = {
    val result = t._2

    FieldsFromRow(Bytes.toString(t._1.get()),
      HBaseHelper.getSafeStringOption(result, "data", "document"),
      HBaseHelper.getSafeStringOption(result, "data", "dataSource"),
      HBaseHelper.getSafeStringOption(result, "data", "createDate"),
      HBaseHelper.getSafeStringOption(result, "data", "language"),
      HBaseHelper.getSafeStringOption(result, "data", "physFormat"),
      HBaseHelper.getSafeStringOption(result, "data", "priFormat"),
      HBaseHelper.getSafeStringOption(result, "data", "publisher"),
      HBaseHelper.getSafeStringOption(result, "data", "workId")
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
