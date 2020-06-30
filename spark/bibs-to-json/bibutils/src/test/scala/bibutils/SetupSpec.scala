package bibutils

import org.scalatest.FunSuite

class SetupSpec extends FunSuite {
    test("we can setup a test") {
        assert("one" == "one")
    }

    case class Person(name: String)
    test("A person has a name"){
        val bob = Person("Bob")
        assert(bob.name == "Bob")
    }
}