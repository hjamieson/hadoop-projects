package reader

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec

class SetupSpec extends FlatSpec {
  "2 + 4" should "equal 4" in {
    assert(4 == 2 + 2)
  }

  "Declaring a SparkConf" should "get an object" in {
    val conf = new SparkConf().setAppName("molly").setMaster("local")
    val sc = new SparkContext(conf)
    assert(sc.appName == "molly")
    sc.stop()

  }

}
