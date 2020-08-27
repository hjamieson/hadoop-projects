package org.oclc.hbase.snoop2

import org.apache.hadoop.hbase.TableName

/**
 * implementors must provide certain pieces of information about a table
 * that snoop2 processes need.
 */
trait TableInfo {
  def tableName: TableName
  def startKeys: List[String]

}
