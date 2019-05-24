package reader

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.{SparkConf, SparkContext}

object HBaseReader {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HBaseReader"))

    /*
     * create a config, then a connection, then grab the table:
     */
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)

  }

}
