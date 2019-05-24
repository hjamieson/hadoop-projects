package code.aws

import org.apache.spark.sql.SparkSession

object ReadCSV {

  def main(args: Array[String]): Unit = {
    assert(args.size == 1, "input file path argument missing")
    val spark = SparkSession
      .builder()
      .appName("AWS Code ReadCSV")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("CSV").option("header", true).load(args(0))
    df.show(10)
  }

}
