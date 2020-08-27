package org.oclc.hbase.snoop2.impl

import scala.sys.runtime

object ShutDownTest extends App {
  val closer:Runnable = new Runnable {
    override def run() = println("haha from closer!")
  }
  runtime.addShutdownHook(new Thread(closer))
  Thread.sleep(1000)
  println("main ended")
}
