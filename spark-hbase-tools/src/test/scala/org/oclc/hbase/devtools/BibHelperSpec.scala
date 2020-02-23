package org.oclc.hbase.devtools

import org.oclc.hbase.spark.job.serializer.BibHelper
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class BibHelperSpec extends FlatSpec with Matchers {
  val cdf1 = Source.fromURL(getClass.getResource("/sample1.cdf")).getLines().mkString
  val cdf2 = Source.fromURL(getClass.getResource("/sample2.cdf")).getLines().mkString
  val bib1 = BibHelper("10", cdf1)
  val bib2 = BibHelper("20", cdf2)

  "sample1" should "return a title if found" in {
    assert(bib1.title.get == "The Rand McNally book of favorite pastimes /illustrated by Dorothy Grider.")
  }
  "sample1" should "not return an author" in {
    assert(bib1.author.isEmpty)
  }
  it should "return a pub date" in {
    assert(bib1.publicationDate.nonEmpty)
    println(bib1.publicationDate.get)
  }
  it should "have a size of 7777" in {
    assert(bib1.documentSize == 7777)
  }
  it should "have a key of 10" in {
    assert("10" == bib1.key)
  }

  "sample2" should "return a title if found" in {
    assert(bib2.title.get == "Bacteriophages for Bacillus pumilus from an animal waste lagoon /by Kenneth Edward Irmen.")
  }
  "sample2" should "return a valid author" in {
    assert(bib2.author.isDefined)
    assert(bib2.author.get == "Irmen, Kenneth E.")
  }
  it should "have a size of 2248" in {
    assert(bib2.documentSize == 2248)
  }
  it should "have a key of 20" in {
    assert("20" == bib2.key)
  }



}
