package org.oclc.hbase.devtools

import org.oclc.hbase.spark.job.TableStartStopOptions
import org.scalatest.{FlatSpec, Matchers}

class CliOptionSpec extends FlatSpec with Matchers {

  "An options class" should "provide help" in {
    val args = "--help".split(" ")
    assertThrows[IllegalArgumentException] {
      val cli = new TableStartStopOptions(args)
    }
  }

  "An options class" should "ask for table" in {
    val args = "-t footable -s 1 -e 2 -o out".split(" ")
    val cli = new TableStartStopOptions(args)
    assert(cli.table() == "footable")
  }

  "An options class" should "allow start key" in {
    val args = "-t footable -s abc -e def -o out".split(" ")
    val cli = new TableStartStopOptions(args)
    assertResult("abc") {
      cli.startKey()
    }
  }

  "An options class" should "throw with invalid option" in {
    val args = "-x footable  -s 1 -e 2 -o out".split(" ")
    assertThrows[IllegalArgumentException] {
      val cli = new TableStartStopOptions(args)
    }
  }

}
