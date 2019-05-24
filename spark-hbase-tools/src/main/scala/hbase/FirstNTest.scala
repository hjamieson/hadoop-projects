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
object FirstNTest {
  val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val cli = new FirstNTestCliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("Table Reader"))
    sc.setLogLevel("ERROR")

    val hBaseConf = HBaseConfiguration.create()
    val tableName = TableName.valueOf(cli.table())
    // collection region information
    val con = ConnectionFactory.createConnection(hBaseConf)
    val regions = con.getRegionLocator(tableName).getAllRegionLocations()
    val regionData = for (r <- regions.asScala) yield RegionData(r)

    regionData foreach println

    // get the 1st N keys from each region
    regionData.foreach(r => {
      r.keys = getNKeys(r, cli.family.getOrElse("data"), 3)
    })

    // generate timing data
    val hTable = con.getTable(tableName)
    val obs = regionData.flatMap(r => {
      queryTest(r, con, hTable, cli.family.getOrElse("data"))
    })

    sc.parallelize(obs).map(JsonMapper.get.writeValueAsString(_)).saveAsTextFile(cli.outputDir())
    con.close()
  }

  def queryTest(rgn: RegionData, conn: Connection, hTable: Table, family: String): List[Observation] = {
    rgn.keys.map { key =>
      val get = new Get(key)
      get.addFamily(Bytes.toBytes(family))
      val time = System.currentTimeMillis()
      val result = hTable.get(get)
      val elapsed = System.currentTimeMillis() - time
      Observation(rgn.name, key, time, elapsed)
    }
  }


  def getNKeys(rgnData: RegionData, family: String, n: Int): List[Array[Byte]] = {
    LOG.info("generating keys for {}", rgnData.name)
    val conn = ConnectionFactory.createConnection()
    try {
      val t = conn.getTable(TableName.valueOf(rgnData.table))
      val scan = new Scan()
      scan.setStartRow(rgnData.start)
      scan.setStopRow(rgnData.end)
      scan.setCaching(n)
      scan.addFamily(Bytes.toBytes(family))
      scan.setFilter(new FirstKeyOnlyFilter())
      val scanner = t.getScanner(scan)
      val rows = scanner.next(n)
      rows.map(r => r.getRow()).toList
    } catch {
      case e: Exception => List()
    } finally {
      conn.close
    }
  }

}

case class RegionData(start: Array[Byte], end: Array[Byte], name: String, var keys: List[Array[Byte]] = List()) {
  override def toString: String = "%s\t%s\t%s".format(
    name,
    Bytes.toString(start),
    Bytes.toString(end)
  )

  def table: String = name.split(",")(0)
}

object RegionData {
  def apply(hrl: HRegionLocation): RegionData = {
    hrl.getRegionInfo.getStartKey
    new RegionData(hrl.getRegionInfo.getStartKey, hrl.getRegionInfo.getEndKey, hrl.getRegionInfo.getRegionNameAsString)
  }
}

case class Observation(region: String, key: Array[Byte], time: Long, elapsed: Long) {
  override def toString: String = {
    "%s\t%s\t%d\t%d".format(region,
      Bytes.toHex(key),
      time,
      elapsed)
  }
}

class FirstNTestCliOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val startKey = opt[String](descr = "start row", short = 's', required = false)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = false)
  val outputDir = opt[String](descr = "output directory", short = 'o', required = true)
  val family = opt[String](descr = "column family", short = 'f', required = false)
  verify()
}

