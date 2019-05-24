package reader

import org.apache.spark.{SparkConf, SparkContext}

/**
  * read a table in hbase and count the rows
  */
object HdfsRowCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Row Counter")
//      .setMaster("local")

    val sc = new SparkContext(sparkConf)

    val rows = sc.textFile(args(0)).count()
    println(s"$rows rows")

    sc.stop()

  }
}
