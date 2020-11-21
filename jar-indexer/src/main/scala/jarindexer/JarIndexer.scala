package jarindexer

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.util.jar.JarInputStream
import java.util.zip.{ZipEntry, ZipInputStream}

import scala.annotation.tailrec

class JarIndexer(val jarPath: String, filter:(ZipEntry)=> Boolean) {
  val jarFile = new File(jarPath)
  if(!jarFile.exists()) throw new FileNotFoundException()

  def entries: List[Entry] = {
    @tailrec
    def helper(jar: JarInputStream, zipEntry: ZipEntry, accum: List[Entry]): List[Entry] = {
      if (zipEntry == null) accum
      else if (filter(zipEntry)) helper(jar, jar.getNextEntry, Entry(zipEntry.getName, jarFile.getName) :: accum)
      else helper(jar, jar.getNextEntry, accum)
    }
    val z1 = new JarInputStream(new BufferedInputStream(new FileInputStream(new File(jarPath))))
    val result = helper(z1, z1.getNextEntry, List.empty)
    z1.close()
    result
  }
}

object JarIndexer {
  def apply(jarPath: String): JarIndexer = new JarIndexer(jarPath, (_)=> true)
  def apply(jarPath: String, filter:(ZipEntry)=>Boolean): JarIndexer = new JarIndexer(jarPath, filter)
}


case class Entry(val name: String, val jar: String = "")

object Entry {
  def apply(ze: ZipEntry): Entry = {
    new Entry(ze.getName)
  }
}