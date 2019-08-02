import Dependencies._

ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.oclc"
ThisBuild / organizationName := "OCLC"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-perf-tools",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      kafka % Compile
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
