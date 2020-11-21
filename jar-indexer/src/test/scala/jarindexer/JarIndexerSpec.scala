package jarindexer

import java.io.FileNotFoundException

import org.scalatest.flatspec.AnyFlatSpec

class JarIndexerSpec extends AnyFlatSpec {
  val testJarPath = "src/test/resources/hadoop-common-2.7.3.jar"

  "A JarIndexer object" should "create a ndxr" in {
    val ndxr = JarIndexer(testJarPath)
    assert(ndxr.jarPath == testJarPath)
  }

  it should "confirm jar exists" in {
    assertThrows[FileNotFoundException]{
      JarIndexer("path/to/jar")
    }
  }
  it should "return a list of entries" in {
    assert(JarIndexer(testJarPath).entries.size > 0)
  }
  it should "handle filters" in {
    assert(JarIndexer(testJarPath, (_)=> true).entries.size > 0)
    assert(JarIndexer(testJarPath, (_)=> false).entries.size == 0)
  }

}
