package org.oclc.hadoop.setup

import java.util.Date

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Read the sample dictionary, load in a dataframe, then print it out
  */
object Example1 extends App {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Simple Setup Test")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val d = Dictionary(getClass.getResourceAsStream("/greek-names.txt"))
val random = new Random(new Date().getTime)
  val entries = d.size

  val ddd = spark.sparkContext.parallelize(1 to 10).map {
    n =>
      val author = d.getName(random.nextInt(d.size))
      val title = d.getMeaning(random.nextInt(d.size)) + " " + d.getMeaning(random.nextInt(d.size))
      (author, title)
  }

//  ddd foreach println
  ddd.toDF("author","title").show(true)

}
