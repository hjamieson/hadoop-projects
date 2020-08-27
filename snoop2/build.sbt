name := "snoop2"
organization := "org.oclc"
version := "0.1"
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.2",
  "org.apache.hbase" % "hbase-client" % "1.2.0" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)

assemblyMergeStrategy in assembly := {
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}