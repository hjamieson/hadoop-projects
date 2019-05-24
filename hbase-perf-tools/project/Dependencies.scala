import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val hbaseClient = "org.apache.hbase" % "hbase-client" % "1.2.0"
  lazy val hbaseServer = "org.apache.hbase" % "hbase-server" % "1.2.0"
  lazy val hbaseCommon = "org.apache.hbase" % "hbase-common" % "1.2.0"
  lazy val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % "2.6.5"
  lazy val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.6.5"
}
