package org.oclc.hbase.perf.spark

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}

object TestForHoldings extends App {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "hddev1db001dxc1.dev.oclc.org")
  val conn = ConnectionFactory.createConnection(conf)
  val worldcat = conn.getTable(TableName.valueOf("Worldcat"))
  val get = new Get(Bytes.toBytes("1"))
  get.addFamily(Bytes.toBytes("data"))
  val result = worldcat.get(get)
  if (result.isEmpty) {
    println("result is empty!")
  } else {
    while (result.advance()) {
      val cel = result.current()
      val qualifier = CellUtil.cloneQualifier(cel)
//      if (qualifier.startsWith(Bytes.toBytes("hold:"))) println(Bytes.toString(qualifier))
      println(Bytes.toString(qualifier))
    }
  }
  worldcat.close()
  conn.close()


}
