package jobs

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64

object HBaseHelper {

  /**
   * converts an HBase Scan to a base64 string to be used with newAPIHadoopRDD.
   *
   * @param scan an HBase Scan
   * @return String
   */
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }

  /**
   * provides a safe way to call for a column value that may not exist in the
   * Result.  Since getValue returns Array[Byte] (which could be null), and we
   * desire a string, we can wrap this safely with an option.
   * @param fam column family
   * @param qual column qualifier
   * @return an Option[String] from the byte array
   */
  def getSafeStringOption(result: Result, family: String, qualifier: String): Option[String] = {
    try {
      Some(new String(result.getValue(family.getBytes(), qualifier.getBytes)))
    } catch {
      case _:Throwable => None
    }
  }

}
