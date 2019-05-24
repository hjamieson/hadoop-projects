package code.aws

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BathSpec extends FlatSpec {
  //  "The Bath File" should "have 201 rows of data minus header" in {
  val spark = SparkSession.builder()
    .appName("read bath file")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  //id,uri,title,published,isbn,author
  val bathSchema = StructType(Array(
    StructField("id", StringType, false),
    StructField("uri", StringType, false),
    StructField("title", StringType, false),
    StructField("published", StringType, false),
    StructField("isbn", StringType, false),
    StructField("author", StringType, false)
  ))
  val df = spark.read.format("CSV").option("header", true).schema(bathSchema).load("src/test/resources/bath-books.csv")
  //  df.show(10)

  "The Bath File" should "have 201 rows of data minus header" in {
    assert(df.count == 201)
  }

  it should "have 18 rows with author starts with B" in {
    assert(df.filter("author like 'B%'").count == 18)
  }

  df.show(15)


}
