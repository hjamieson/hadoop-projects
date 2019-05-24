package org.oclc.hadoop.setup

import org.scalatest.FlatSpec

import scala.io.Source

class TextSpec extends FlatSpec {
  "The parser" should "parse the text file into words" in {
    val bs = Source.fromInputStream(getClass.getResourceAsStream("/sample-text.txt"))
    val lines = bs.getLines().toList
    bs.close
    val stops: List[String] = StopWords.apply
    val words = lines.flatMap(_.split("\\s+|,")).filter(_.trim().length > 0).distinct.filter(!stops.contains(_))
    println(s"${words.size} words decoded")
    words.take(15) foreach println
    assert(words.size > 0)
  }

  "A StopWords object" should "return a list of stop words" in {
    val stops = StopWords(getClass.getResourceAsStream("/stopwords.txt"))
    assert(stops.isInstanceOf[List[String]])
    assert(stops.size > 0)
    assert(stops.contains("and"))
    assert(stops.contains("it"))
  }

}
