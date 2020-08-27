package org.oclc.hbase.snoop2

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class SetupSpec extends AnyFlatSpec with BeforeAndAfterAll {
  var conn: Connection = _
  var admin: Admin = _

  override def beforeAll(): Unit = {
    conn = ConnectionFactory.createConnection()
    admin = conn.getAdmin
    super.beforeAll()
  }

  "this setup" should "create a new hbase connection" in {
    assert(!conn.isClosed)
  }
  it should "have an admin" in {
    assert(admin.isInstanceOf[Admin])
  }
  it should "find a table" in {
    val list = admin.listTableNames()
    assert(list.size > 0)
    println(s"# of tables = ${list.size}")
  }
  it should "be able to get the hytest table" in {
    val worldcat = conn.getTable(TableName.valueOf("Worldcat"))
    val result = worldcat.get(new Get("1".getBytes()))
    assert(!result.isEmpty)
    println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("workId"))))
    worldcat.close()
  }

  override protected def afterAll(): Unit = {
    admin.close()
    conn.close()
  }
}
