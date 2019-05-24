package example

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json._

/**
  * Description:
  * reads some data on HDFS and imports it into a table in HBase.
  *
  * Args:
  * <input-file> a JSON flle to read.
  * <table> the hbase table to store the data into.
  */
object ImportData {
  def main(args: Array[String]): Unit = {
    require(args.length > 1, "args: <input-json(hdfs)> <target-table(hbase)>")
    val inputJson = args(0)
    val table = args(1)

    // initialize the spark engine
    val sparkConf = new SparkConf().setAppName("HBase Table Import")
    val sc = new SparkContext(sparkConf)

    // load the data from the file in HDFS (key <tab> {json})
    val rdd = sc.textFile(inputJson).map(j => {
      val parts = j.split(raw"\t")
      val m = JSON.parseFull(parts(1)).get.asInstanceOf[Map[String,Any]]
      m + ("key"-> parts(0))  // tack the key onto the map
    })


    // push the data to HBase
    rdd.foreachPartition { part =>
      val hbc = HBaseConfiguration.create()
      hbc.addResource("hbase-site.xml")
      val conn = ConnectionFactory.createConnection(hbc)
      val htable = conn.getTable(tn(table))

      part.foreach(rec => {
        val put = makePut(rec, "d".getBytes())  // don't mind the static CF!  I am lazy!
        htable.put(put)
      })

      htable.close()
      conn.close()
    }

  }

  /**
    * creates a put using the given key as the rowkey, and the map as the columns.  We assume
    * the CF is "d", and the map has a "key" value to use as the rowkey.
    *
    * @param record
    * @param key
    * @return
    */
  def makePut(record: Map[String, Any], cf: Array[Byte]): Put = {
    val put = new Put(record("key").toString.getBytes())
    record.seq.filter(_._1 != "key").foreach(t => {
      put.addColumn(cf, t._1.toString.getBytes(), t._2.toString.getBytes())
    })
//    put.addColumn(cf, "f1".getBytes, "data".getBytes())
    put
  }

  def tn(strTableName:String): TableName = TableName.valueOf(strTableName)

}