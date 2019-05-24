import Dependencies._

ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.oclc.hbase"
ThisBuild / organizationName := "OCLC.org"

lazy val root = (project in file("."))
  .settings(
    name := "hbase-perf-tools",
    libraryDependencies += scalaTest % Test
  )
  .aggregate(scalaTools, scalaDemo)

lazy val scalaTools = (project in file("scala-tools"))
   .settings(
     name := "scala-tools",
    libraryDependencies += scalaTest % Test
   )

lazy val scalaDemo = (project in file("scala-demo"))
   .settings(
     name := "scala-demo",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      hbaseClient,
      hbaseServer,
      hbaseCommon,
      hadoopClient
    )
   )

lazy val sparkTools = (project in file("spark-tools"))
   .settings(
     name := "spark-tools",
    libraryDependencies += scalaTest % Test
   )