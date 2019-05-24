package example

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Reads a table and counts the number of rows.
  * args: table (fully qualified)
  */
object RowCounter {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "args: <ns:table>" )

    val scf = new SparkConf().setAppName(s"Reading Table ${args(0)}")
    val sc = new SparkContext(scf)
    val spark = SparkSession.builder().config(scf)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE, args(0))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    println(s"*** number of records = ${hbaseRDD.count()} ***")

  }

}
