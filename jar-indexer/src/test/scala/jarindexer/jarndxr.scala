package jarindexer

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.util.jar.JarInputStream

import org.scalatest.flatspec.AnyFlatSpec

class ReadJarSpec extends AnyFlatSpec {
  val jarFilePath = "src/test/resources/hadoop-common-2.7.3.jar"

  "A Jar file" should "be opened and closed" in {
    val bis = new BufferedInputStream(new FileInputStream(new File(jarFilePath)))
    val zis = new JarInputStream(bis)
    var entry = zis.getNextEntry
    var count = 0
    while (entry != null){
//      println(s"entry => ${entry.getName}")
      count += 1
      entry = zis.getNextEntry
    }
    zis.close()
    assert(count > 0)
  }

  "Jar entries" should "can be filtered for files" in {
    val z1 = new JarInputStream(new BufferedInputStream(new FileInputStream(new File(jarFilePath))))
    var e1 = z1.getNextEntry
    var c1 = 0
    while (e1 != null){
      c1 += 1
      e1 = z1.getNextEntry
    }
    z1.close()
    assert(c1 > 0)
    println(s"with dirs = $c1")
    val z2 = new JarInputStream(new BufferedInputStream(new FileInputStream(new File(jarFilePath))))
    var e2 = z2.getNextEntry
    var c2 = 0
    while (e2 != null) {
      if (!e2.isDirectory) c2 += 1
      e2 = z2.getNextEntry
    }
    z2.close
    println(s"without dirs=$c2")
    assert(c2 < c1)
  }

}
