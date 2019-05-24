ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "com.example"

lazy val hello = (project in file("."))
  .settings(
    name := "hbase-spark-reader"
  )

resolvers += "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

//libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.12.1" notTransitive()
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.12.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.12.1"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.12.1"
