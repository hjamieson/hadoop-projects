package org.oclc.hbase.perf.spark

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class SetupSpec extends FlatSpec with Matchers with BeforeAndAfter {

  import org.apache.spark.sql.functions._

  var spark: SparkSession = null

  before {
    spark = SparkSession.builder()
      .appName("setup test")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
  }

  after {
    spark.close()
  }

  "a Spark job" should "create a sparkSession" in {
    val evens = spark.range(100).filter(pmod(col("id"),lit(2)) === 0)
    assert(evens.count() == 50)
  }

}
