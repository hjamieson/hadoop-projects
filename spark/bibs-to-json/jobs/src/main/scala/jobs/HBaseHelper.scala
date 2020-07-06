package jobs

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}

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


  case class FieldsFromRow(rowkey: String, document: String)

  val DATA = Bytes.toBytes("data")
  val DOCUMENT = Bytes.toBytes("document")

  def rowToFields(t: (ImmutableBytesWritable, Result)): FieldsFromRow = {
    val rowkey = Bytes.toString(t._1.get())
    val doc: Option[Array[Byte]] = Some(t._2.getValue(DATA, DOCUMENT))
    if (doc.isEmpty) println(s"missing document for row $rowkey")
    FieldsFromRow(rowkey, Bytes.toString(doc.getOrElse("NA".getBytes)))
  }

}
