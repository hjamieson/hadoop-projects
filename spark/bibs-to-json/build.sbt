ThisBuild / name := "bibs2json"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "org.oclc"
ThisBuild / organizationName := "OCLC.org"

lazy val root = (project in file("."))
    .settings(
        libraryDependencies ++= Seq(
             "org.scalatest" %% "scalatest" % "3.1.2" % Test,
             "org.apache.spark" %% "spark-sql" % "2.4.4" % Compile
        )
    )
    .aggregate(bibutils)

lazy val bibutils = project
    .settings(
        version := "1_a",
        libraryDependencies ++= Seq(
             "org.scalatest" %% "scalatest" % "3.1.2" % Test
        )

    )