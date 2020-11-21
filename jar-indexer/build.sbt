name := "jar-indexer"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.+" % Test
)

test in assembly := {}
mainClass in assembly := Some("job.Index2Csv")