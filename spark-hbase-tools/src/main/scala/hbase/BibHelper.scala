package hbase

import scala.xml.XML

case class BibHelper(key: String, cdf: String) {
  val root = XML.loadString(cdf)

  def title: Option[String] = {
    val seq = root \ "v245"
    if (seq.isEmpty) None
    else Some(seq.text)
  }

  def author: Option[String] = {
    val seq = root \ "v100"
    if (seq.isEmpty) None
    else Some(seq.text)
  }

  def publicationDate: Option[String] = {
    val seq = root \ "v260" \ "sc" \ "d"
    if (seq.nonEmpty) Some(seq.text)
    else None
  }

  def documentSize: Int = cdf.length

}
