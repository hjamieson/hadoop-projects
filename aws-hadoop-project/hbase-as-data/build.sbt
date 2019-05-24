ThisBuild / scalaVersion := "2.11.8"
ThisBuild / version := "1.0"
ThisBuild / organization := "org.oclc.hadoop"

import Dependencies._

lazy val reader = (project in file("table-reader"))
  .settings(
    name := "table-reader",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      sparkCore % Compile,
      hbaseClient % Compile,
      hbaseServer % Compile
    )
  )

