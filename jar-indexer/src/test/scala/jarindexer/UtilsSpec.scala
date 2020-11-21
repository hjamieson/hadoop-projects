package jarindexer

import org.scalatest.flatspec.AnyFlatSpec

class UtilsSpec extends AnyFlatSpec {

  object ClassNameDecorator {
    val rgx = raw".+/(\w+.class)".r

    def apply(str: String): String = {
      str match {
        case rgx(s) => s"""<span class="hilite">$s</span>"""
        case _ => "na"
      }
    }
  }
  "a decorator" should "hi-lite the class name" in {
    val sample = "org/apache/hadoop/crypto/CryptoProtocolVersion.class"
    val result = ClassNameDecorator(sample)
    assert(result.contains("""<span class="hilite">CryptoProtocolVersion.class</span>"""))
  }

}
