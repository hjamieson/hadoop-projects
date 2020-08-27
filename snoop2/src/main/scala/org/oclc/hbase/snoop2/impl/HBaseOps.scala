package org.oclc.hbase.snoop2.impl

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get, Table}

/**
 * We will put HBase client-specific code here needed by the engine.
 * This may be refactored later.
 */
object HBaseOps {
  /**
   * performs an hbase get using the given table/key, and times the response.
   *
   * @param table
   * @param rowkey
   * @return response time in ms
   */
  def doGetRow(table: Table, rowkey: Array[Byte]): Observation = {
    val startTime = System.currentTimeMillis()
    val get = new Get(rowkey)
    val result = table.get(get)
    Observation(table.getName.getNameWithNamespaceInclAsString, rowkey, startTime, System.currentTimeMillis() - startTime)
  }

  /**
   * returns the start keys for the given table.
   *
   * @param t
   * @return
   */
  def getStartKeys(t: String)(implicit conn: Connection): Array[Array[Byte]] = {
    conn.getRegionLocator(TableName.valueOf(t)).getStartKeys
  }
}
