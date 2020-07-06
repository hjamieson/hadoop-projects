package jobs

import org.scalatest.FunSuite

class BasicSpec extends FunSuite {

  val goodBytes: Option[Array[Byte]] = Option("freddie".getBytes)
  val badBytes: Option[Array[Byte]] = None
  test("we can use flatMap for potential nulls"){

    assert(goodBytes.isDefined)
    assert(badBytes.isEmpty)
    assert(goodBytes.flatMap(b => Some(new String(b))).get == "freddie")
    assert(goodBytes.flatMap(b => Some(new String(b))).get == "freddie")
    assert(badBytes.flatMap(b => Some(new String(b))) == None)

  }

  test("we can handle the empty array of bytes"){
    val nullstr = badBytes.flatMap(b=> Some(new String(b)))
    assert(nullstr.isEmpty)

    val goodstr = goodBytes.flatMap(b => Some(new String(b)))
    assert(goodstr.isDefined)
    assert(goodstr.get == "freddie")
  }

  test("are we using Some() wrong?"){
    val zip = Option(null)
    assert(zip.isEmpty)
  }

  test("can we use Option(null)?"){
    val javaNull: Array[Byte] = null
    val javaNullOption = Option(javaNull)

    assert(Option(javaNull).isEmpty)
    assert(javaNullOption.isEmpty)

    val nullString: Option[String] = Option(null)
    case class Person(val name: Option[String])
    val p = Person(nullString)
    assert(p.name.isEmpty)

  }

}
