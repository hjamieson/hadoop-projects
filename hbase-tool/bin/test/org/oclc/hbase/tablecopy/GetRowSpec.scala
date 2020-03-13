package org.oclc.hbase.tablecopy

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FlatSpec

class GetRowSpec extends FlatSpec {
  val hcf = HBaseConfiguration.create()
  hcf.set("hbase.zookeeper.quorum", "hdchbadb001pxh1.csb.oclc.org:2181,hdchbadb002pxh1.csb.oclc.org:2181")
  val con = ConnectionFactory.createConnection(hcf)
  val tableName = TableName.valueOf("Worldcat")
  val table = con.getTable(tableName)

  val row = table.get(new Get(Bytes.toBytes("1")))
  assert(!row.isEmpty)
  println(s"returned row has size=${row.size()}")

  table.close()
  con.close()

}
