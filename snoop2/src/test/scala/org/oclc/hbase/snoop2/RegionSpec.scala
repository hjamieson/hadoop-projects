package org.oclc.hbase.snoop2

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class RegionSpec extends AnyFlatSpec with BeforeAndAfterAll {
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
  "a connection" should "provide a RegionLocator" in {
    val locator = conn.getRegionLocator(TableName.valueOf("Worldcat"))
    val startKeys = locator.getStartKeys
    assert(startKeys.length !=0)
    println(s"number of regions = ${startKeys.length}")
    startKeys.take(5).map(Bytes.toString(_)) foreach println
    println("end keys")
    locator.getEndKeys.take(5).map(Bytes.toString(_)) foreach println
  }


  override protected def afterAll(): Unit = {
    admin.close()
    conn.close()
  }
}
