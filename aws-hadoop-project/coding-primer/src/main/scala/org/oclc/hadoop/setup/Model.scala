package org.oclc.hadoop.setup

case class Bib(id: Int, title: String, author: String, pubDate: PublicationDate, holdings: List[Holding]) {

}

case class PublicationDate(year: Int, month: Int)

case class Holding(symbol: String, heldSince: PublicationDate)
