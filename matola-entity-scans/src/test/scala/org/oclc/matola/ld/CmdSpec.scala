package org.oclc.matola.ld

import org.rogach.scallop.ScallopConf
import org.scalatest.{FlatSpec, Matchers}

class Opts(args: Seq[String]) extends ScallopConf(args) {
  val table = opt[String](short = 't')
  verify()
}

class CmdSpec extends FlatSpec with Matchers {
  "A Option" should "accept the table option" in {
    val opts = new Opts("-t panda".split(" "))
    assert(opts.table() == "panda")
  }

}
