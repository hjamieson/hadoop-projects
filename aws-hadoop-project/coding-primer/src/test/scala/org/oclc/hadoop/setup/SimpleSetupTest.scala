package org.oclc.hadoop.setup

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleSetupTest extends FlatSpec {
  "A SparkSession" should "be returned for testing" in {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Simple Setup Test")
      .getOrCreate()
    assert(spark != null)
  }

  "A Spark DataFrame" should "be selectable" in {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Simple Setup Test")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val df = Seq((1, "abc"), (2, "def"), (3, "ghi")).toDF("id", "value")

    df.printSchema()
    df.show()
    val result = df.filter("id == 3").select('value).collect()
    assert(result.size == 1)
    assert(result(0)(0) == "ghi")
  }

}
