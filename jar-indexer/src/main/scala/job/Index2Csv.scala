package job

import java.util.zip.ZipEntry

import jarindexer.JarIndexer

object Index2Csv extends App {

  object ClassNameDecorator {
    val rgx = raw"(.+/)(\w+.class)".r

    def apply(str: String): String = {
      str match {
        case rgx(pre, s) => s"""$pre<span class="hilite">$s</span>"""
        case _ => str
      }
    }
  }


  def toCSV(j: JarIndexer): Unit = {
    // output rows
    j.entries.foreach(e => {
      println(s"${e.name}, ${e.jar}")
    })
  }


  val filter = (e: ZipEntry) => !e.isDirectory && !e.getName.contains("$")
    println("class,jar")

  args.foreach { jar =>
    toCSV(JarIndexer(jar, filter))
  }

}
