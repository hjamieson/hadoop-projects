import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.oclc.hbase"
ThisBuild / organizationName := "OCLC.org"

lazy val root = (project in file("."))
  .settings(
    name := "table-copier",
    libraryDependencies ++= Seq(
        scalaTest % Test,
        hbaseClient % Compile,
        hbaseServer % Compile,
        hbaseCommon % Compile,
        hadoopCommon % Compile
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
