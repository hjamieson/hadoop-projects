package org.oclc.hadoop.setup

import java.io.{File, FileInputStream, InputStream}

import org.oclc.hadoop.setup.StopWords.getClass

import scala.io.Source

/**
  * This class holds the words we will be using to generate random data
  */
class Dictionary(pairs: Array[(String, String)]) {

  def getName(n: Int): String = pairs(n)._1

  def getMeaning(n: Int): String = pairs(n)._2

  def size: Int = pairs.size
}

object Dictionary {
  def apply(textStream: InputStream): Dictionary = {
    val bs = Source.fromInputStream(textStream)
    val text = bs.getLines.toList
    bs.close

    val p = text.head.split("\u2013")
    assert(p.size == 2)
    val pairs = text.map(_.split("\u2013")).map(t => (t(0).trim, t(1).trim)).toArray
    new Dictionary(pairs)
  }

  def apply(textFile: String): Dictionary = {
    apply(new FileInputStream(textFile))
  }
}

object StopWords {
  def apply(stream: InputStream): List[String] = {
    val bs = Source.fromInputStream(stream)
    val lines = bs.getLines().toList
    bs.close()
    lines.map(_.trim).distinct
  }

  def apply: List[String] = apply(getClass.getResourceAsStream("/stopwords.txt"))
}

object WordList{
  def apply(stream: InputStream): List[String] = {
    val bs = Source.fromInputStream(stream)
    val lines = bs.getLines().toList
    bs.close()
    lines.map(_.trim).distinct
  }
}
