package org.oclc.matola.ld

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class ParseRdfSpec extends FlatSpec with Matchers {
  "The reader" should "be able to load the text file from source" in {
    val file = Source.fromFile("src/test/resources/entity-sample1.txt")
    val splits = file.getLines().map(_.split("\t"))
    assert(splits.size == 5)
    splits.foreach(p => println(p(0)))
//    val types = splits.map(_(1)).map { rdf =>
//
//    }
  }

  "A pattern" should "be repeatedly applied" in {
    val rdfrgx = "<rdf:type rdf:resource=\"(.*)\"/>".r
    val l = rdfrgx.findAllIn("abcdef<rdf:type rdf:resource=\"monty python\"/>abcdef")
    assert(l.size == 1)
    l.foreach{
      l => println(l)
    }
  }
}
