package org.oclc.hbase.snoop2.impl

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.util.Bytes
import org.oclc.hbase.snoop2.TableInfo

case class HBaseTableInfo(val name: String)(implicit connection: Connection) extends TableInfo {
  override def tableName: TableName = TableName.valueOf(name)

  override def startKeys: List[String] = {
    connection.getRegionLocator(tableName)
      .getStartKeys
      .map(Bytes.toString(_))
      .toList
  }
}
