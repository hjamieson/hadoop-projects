package job

import java.util.zip.ZipEntry

import jarindexer.JarIndexer

object IndexJar extends App {

  object ClassNameDecorator {
    val rgx = raw"(.+/)(\w+.class)".r

    def apply(str: String): String = {
      str match {
        case rgx(pre, s) => s"""$pre<span class="hilite">$s</span>"""
        case _ => str
      }
    }
  }


  def toHtml(j: JarIndexer): Unit = {
    println(
      """
        |<table>
        |<tr><th>class</th><th>jar</th></tr>
        |""".stripMargin)
    // output rows
    j.entries.foreach(e => {
      println(s"""<tr><td class="classname">${ClassNameDecorator(e.name)}</td><td class="jar">${e.jar}</td></tr>""")
    })
    println("</table>")
  }

  def printHeader: Unit = {
    println(
      """
        |<html>
        |<head>
        |<style>
        |table {font-size: 1em; font-family: Tahoma, Arial; width: 100%;}
        |th {text-align: left; background-color: cornflowerblue;}
        |td.classname {width: 60%;}
        |.classname {color: navy}
        |span.hilite { font-size: 1.2em; color: maroon;}
        |</style>
        |</head>
        |<body>
        |""".stripMargin)
  }

  def printTrailer: Unit = {
    println("</body></html>")
  }

  //  toMarkDown(JarIndexer(args(0),!_.isDirectory))
  val filter = (e: ZipEntry) => !e.isDirectory && !e.getName.contains("$")
  printHeader

  args.foreach { jar =>
    toHtml(JarIndexer(jar, filter))
  }

  printTrailer

}
