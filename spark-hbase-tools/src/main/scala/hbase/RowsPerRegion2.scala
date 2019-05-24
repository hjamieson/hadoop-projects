package hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HRegionLocation, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Counts rows in each region of an HBase table.
  * args: table to scan
  * options: startKey, endKey
  */
object RowsPerRegion2 {
  val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val cli = new RowsPerRegion2CliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Table Reader"))
    sc.setLogLevel("ERROR")

    val hBaseConf = HBaseConfiguration.create()
    val tableName = TableName.valueOf(cli.table())
    // collection region information
    val con = ConnectionFactory.createConnection(hBaseConf)
    val regions = con.getRegionLocator(tableName).getAllRegionLocations()
    val regionData = for (r <- regions.asScala) yield RegionData2(r)

    // count the rows in each region:
    regionData.foreach(r => {
      countRows(r)
    })

    con.close()

    if (cli.outputDir.isDefined)
      sc.parallelize(regionData).map(JsonMapper.get.writeValueAsString(_)).saveAsTextFile(cli.outputDir())
    else regionData foreach println

  }


  def countRows(rgnData: RegionData2, cacheSize: Int = 1000): Unit = {
    LOG.info("generating keys for {}", rgnData.name)
    val conn = ConnectionFactory.createConnection()
    try {
      val t = conn.getTable(TableName.valueOf(rgnData.table))
      val scan = new Scan()
      scan.setStartRow(rgnData.start)
      scan.setStopRow(rgnData.end)
      scan.setCaching(cacheSize)
      scan.setFilter(new FirstKeyOnlyFilter())
      val scanner = t.getScanner(scan)
      var nxt = scanner.next(cacheSize)
      var nbrRows = 0
      while (nxt.size > 0) {
        nbrRows = nbrRows + nxt.size
        nxt = scanner.next(cacheSize)
      }
      scanner.close()
      rgnData.numRows = nbrRows
    } catch {
      case e: Exception => rgnData.numRows = -1
    } finally {
      conn.close
    }
  }

}

case class RegionData2(start: Array[Byte], end: Array[Byte], name: String, var numRows: Long = 0) {
  override def toString: String = "%s\t%s\t%s\t%d".format(
    name,
    Bytes.toString(start),
    Bytes.toString(end),
    numRows
  )

  def table: String = name.split(",")(0)
}

object RegionData2 {
  def apply(hrl: HRegionLocation): RegionData2 = {
    hrl.getRegionInfo.getStartKey
    new RegionData2(hrl.getRegionInfo.getStartKey, hrl.getRegionInfo.getEndKey, hrl.getRegionInfo.getRegionNameAsString)
  }
}


class RowsPerRegion2CliOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val startKey = opt[String](descr = "start row", short = 's', required = false)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = false)
  val outputDir = opt[String](descr = "output directory", short = 'o', required = false)
  val family = opt[String](descr = "column family", short = 'f', required = false)
  verify()
}

