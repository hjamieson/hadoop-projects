ThisBuild / name := "bibs2json"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "org.oclc"
ThisBuild / organizationName := "OCLC.org"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
    )
  )
  .aggregate(bibutils)

lazy val bibutils = project
  .settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.1.2" % Test,
      "org.scala-lang.modules" %% "scala-xml" % "1.3.0" % Compile

    )

  )

lazy val jobs = project
  .settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.1.2" % Test,
      "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.4" % Provided,
      "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.16.2" % Provided,
      "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.16.2" % Provided,
      "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.16.2" % Provided,
      "org.rogach" %% "scallop" % "3.1.3" % Compile

    ),
    resolvers += "OCLC Artifactory" at "https://artifact-m1.shr.oclc.org/artifactory/internal/",

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )