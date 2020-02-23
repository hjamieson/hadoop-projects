package org.oclc.hbase.spark.job

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.oclc.hbase.spark.utils.HBaseHelper
import org.rogach.scallop.ScallopConf

import scala.util.Random

/**
  * Takes a file of rowkeys and does a Get per row, saving the response time
  * args: table to scan
  * options: startKey, endKey
  */
object GetResponseTime {



  def main(args: Array[String]): Unit = {

    val cli = new GetTestCliOptions(args)
    val sc = new SparkContext(new SparkConf().setAppName("GetTest"))
    val hBaseConf = HBaseConfiguration.create()

    // run a scan to generate some keys to use for sampling
    val tableArg = cli.table()
    val family = cli.family.getOrElse("data")
    val sampling = cli.sampling.getOrElse(.10)

    // generate sample keys
    val scan: Scan = new Scan()
    scan.setCaching(1000)   // this is critical
    scan.setCacheBlocks(false)
    scan.setFilter(new FirstKeyOnlyFilter())
    if (cli.startKey.isDefined) scan.setStartRow(cli.startKey().getBytes())
    if (cli.stopKey.isDefined) scan.setStopRow(cli.stopKey().getBytes())
    hBaseConf.set(TableInputFormat.INPUT_TABLE,cli.table())
    hBaseConf.set(TableInputFormat.SCAN, HBaseHelper.convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    /*
     * technique: as keys are returned, assign and random number to each, then
     * sort to give you a random order.
     */
    val rand = new Random()
    val rowkeys = hbaseRDD.map(t => Bytes.toString(t._1.get()))
      .sample(false, sampling)
      .map(p => (p, rand.nextInt(1000000)))
      .sortBy(s => s._2)
      .map(_._1)



    // sample the gets by partiion
    val observations = rowkeys.mapPartitions(iter => {
      val conn = ConnectionFactory.createConnection()
      val tableName = getTableName(tableArg)
      val regionLocator = conn.getRegionLocator(tableName)
      val rgns = iter.map(k => {
        val hregionLocation = regionLocator.getRegionLocation(Bytes.toBytes(k))
        val table = conn.getTable(tableName)
        Obs(k, hregionLocation.getRegionInfo.getRegionNameAsString, doGet(k, table, family))
      }).toList
      conn.close()
      rgns.toIterator
    })

    // write the regions to a file for now
    observations
      .saveAsTextFile(cli.outputDir())
  }

  // do a get of the row and record how long it takes.
  // todo most args get converted to bytes; might make that a requirement
  def doGet(key: String, table: Table, family: String): Long = {
    val get = new Get(Bytes.toBytes(key))
      .addFamily(Bytes.toBytes(family))
    val start = System.currentTimeMillis()
    val result = table.get(get)
    val elapsed = System.currentTimeMillis() - start
    if (!result.isEmpty) elapsed else 0
  }

  def getTableName(s: String): TableName = TableName.valueOf(s)

  case class Obs(key: String, region: String, ms: Long = 0L){
    override def toString: String = s"$key\t$region\t$ms"
  }
}

class GetTestCliOptions(args: Seq[String]) extends ScallopConf(args) {

  override def onError(e: Throwable): Unit = {
    e match {
      case _ => printHelp(); throw new IllegalArgumentException(e.getMessage)
    }
  }

  val table = opt[String](descr = "the name of the table to scan", required = true)
  val sampling = opt[Double](descr = "sampling percentage", short = 'p', required = true)
  val startKey = opt[String](descr = "start row", short = 's', required = false)
  val stopKey = opt[String](descr = "stop row", short = 'e', required = false)
  val outputDir = opt[String](descr = "output directory", short = 'o', required = true)
  val family = opt[String](descr = "ColumnFamily", short = 'f', required = false)
  verify()
}

