package org.oclc.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

import scala.util.{Failure, Success, Try}


object ListTables {
  def loan[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }

  def main(args: Array[String]): Unit = {
    val hcf = HBaseConfiguration.create()
    hcf.set("hbase.zookeeper.quorum", "hdchbadb001pxh1.csb.oclc.org:2181,hdchbadb002pxh1.csb.oclc.org:2181")

    val result: List[String] = loan(ConnectionFactory.createConnection(hcf)){
      con =>
        val admin = con.getAdmin()
        admin.listTableNames().map(_.getNameWithNamespaceInclAsString()).toList
    }

    result.foreach(println)
  }

}
