package code.aws

/**
  * We put helper functions that can be shared here.
  */
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64

object HbaseHelper {

  /**
    * converts an HBase Scan to a base64 string to be used with newAPIHadoopRDD.
    * @param scan an HBase Scan
    * @return String
    */
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }


}
