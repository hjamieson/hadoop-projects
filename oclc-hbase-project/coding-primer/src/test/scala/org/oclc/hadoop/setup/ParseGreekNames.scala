package org.oclc.hadoop.setup

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ParseGreekNames extends FlatSpec with Matchers {
  "A method" should "parse greek names from a file" in {
    val bs = Source.fromFile("src/test/resources/greek-names.txt","UTF-8")
    val text = bs.getLines.toList
    bs.close

    assert(text.size > 0)
    assert(text.head.contains("Achilles"))

    assert(text.head.contains("\u2013"))  // this txt file use unicode em dash (e2 80 93)
    val p = text.head.split("\u2013")
    assert(p.size == 2)
    val pairs = text.map(l => l.split("\u2013")).map(t => (t(0).trim,t(1).trim))
    assert(pairs.size > 0)
  }

  "A Dictionary" should "parse the given input file" in {
    val dict = Dictionary("src/test/resources/greek-names.txt")
    assert(dict.size > 0)
    assert(dict.getName(4) == "Basil")
    assert(dict.getMeaning(4) == "King-like")
    assert(dict.getName(38) == "Icarus")
    assert(dict.getMeaning(38) == "Legendary figure")
  }


}
