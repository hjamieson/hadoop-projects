package code.aws

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LoadSpec extends FlatSpec {
  val outPath = "build/tmp/pants.json"
  // delete the output if it exists
  val outfile = new java.io.File(outPath)
  if (outfile.exists()) {
    FileUtils.deleteDirectory(outfile)
  }


  "A Spark app" should "initialize correctly" in {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("dummy")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json("src/test/resources/dummy.json")
    df.printSchema()

    df.where('gender === "Male")
      .select('buzzword)
      .write.format("json")
      .save(outPath)
  }
}