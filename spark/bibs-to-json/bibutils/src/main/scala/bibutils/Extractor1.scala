package bibutils

import scala.xml.{Elem, NodeSeq}

/**
 * Extracts the specified fields from the XML/CDF and returns
 * an object we can use for serialization
 */

case class Extractor1(val title: String, val author: String, val country: String,
                      val placeOfPub: String,
                      val nameOfPub: String,
                      val dateOfPub: String,
                      val stdrt: String,
                      val stdrt2: String)

object Extractor1 {
  def apply(cdf: Elem): Extractor1 = {

    // the title
    val v245 = cdf \ "v245" \ "sa" \ "d"
    val title = if (v245.isEmpty) "N/A" else v245.text

    // author
    val v100 = cdf \ "v100" \ "sa" \ "d"
    val author = if (v100.isEmpty) "N/A" else v100.text

    // country
    val v257 = cdf \ "v257" \ "sa" \ "d"
    val country = if (v257.isEmpty) "na" else v257.text

    // publisher data
    val v260 = cdf \ "v260"
    val placeOfPub = v260 \ "sa" \ "d" text
    val nameOfPub = v260 \ "sb" \ "d" text
    val dateOfPub = v260 \ "sc" \ "d" text

    // admin fields
    val admin = cdf \ "Admin" \ "OCLCDef"
    val stdrt = admin \ "StdRT" match {
      case a: Elem => a.text
      case _ => "na"
    }
    val stdrt2 = admin \ "StdRT2" match {
      case a: Elem => a.text
      case _ => "na"
    }

    new Extractor1(title, author, country, placeOfPub, nameOfPub, dateOfPub, stdrt, stdrt2)
  }
}
