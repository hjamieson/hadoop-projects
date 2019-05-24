package example

import java.io.IOException

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Description:
  * reads an hbase table by partition
  *
  * Args:
  * <table> the hbase table to scan
  * <out> a path to write the output
  */
object ByPartition {
  final val LOG = LoggerFactory.getLogger(ByPartition.getClass)

  def main(args: Array[String]): Unit = {
    require(args.length > 1, "args: <target-table(hbase)> <output-path>")
    val table = args(0)
    val outputPath = args(1)

    LOG.info(s"start process, table=$table, output=$outputPath")

    // initialize the spark engine
    val sparkConf = new SparkConf().setAppName("HBase Table Import")
    val sc = new SparkContext(sparkConf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table)
    val scan = new Scan()
    scan.setFilter(new FirstKeyOnlyFilter())
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val rdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])


    // dump the key
    rdd.map(ib => new String(ib._1.get())).saveAsTextFile(outputPath)

  }


  /**
    * Writes the given scan into a Base64 encoded string.
    *
    * @param scan The scan to write out.
    * @return The scan saved in a Base64 encoded string.
    * @throws IOException When writing the scan fails.
    */
  @throws[IOException]
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


  def tn(strTableName: String): TableName = TableName.valueOf(strTableName)

}