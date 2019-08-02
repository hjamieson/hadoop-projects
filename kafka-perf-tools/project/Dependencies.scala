import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val kafka = "org.apache.kafka" % "kafka-clients" %"0.10.2.0"
}
