package org.oclc.hbase.snoop2.impl

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.oclc.hbase.snoop2.TableInfo
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class TableInfoSpec extends AnyFlatSpec with BeforeAndAfterAll {
  implicit var conn: Connection = _
  var tableInfo: TableInfo = _
  val strTableName = "Country"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    conn = ConnectionFactory.createConnection()
  }

  override protected def afterAll(): Unit = {
    conn.close()
    super.afterAll()
  }


  "A TableInfo" should "be created using table name" in {
    tableInfo = new HBaseTableInfo(strTableName)
    assert(tableInfo.tableName == TableName.valueOf(strTableName))
  }
  it should "provide start keys from all regions" in {
    assert(tableInfo.startKeys.length > 0)
    tableInfo.startKeys.foreach(k => println(s"key=$k, len=${k.length}"))
  }

}
