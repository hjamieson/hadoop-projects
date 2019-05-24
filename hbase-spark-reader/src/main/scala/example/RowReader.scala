package example

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Reads the data we previously added to the hbase table.  Assumptions:
  * ) cf = "d"
  * ) rowkey = string integer
  * ) column values are strings
  */
object RowReader {


  def main(args: Array[String]): Unit = {
    require(args.length == 1, "args: <ns:table>")

    val scf = new SparkConf().setAppName(s"Reading Table ${args(0)}")
    val sc = new SparkContext(scf)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE, args(0))

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val ddd = hbaseRDD.map((t: (ImmutableBytesWritable, Result)) => RowResult(t._1, t._2))

    ddd.take(3).foreach(p => println(p))

  }

}

case class RowResult(key: ImmutableBytesWritable, v: Result) {

  def id: String = Bytes.toString(key.get())

  def values(): mutable.Map[String, String] = {
    val m = mutable.Map[String,String]()
    while (v.advance()){
      val cell = v.current()
      m += (Bytes.toString(CellUtil.cloneQualifier(cell)) -> Bytes.toString(CellUtil.cloneValue(cell)))
    }
    m
  }

  override def toString: String = s"RowResult(${id.toString} => ${values.toString})"
}
