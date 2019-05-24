import sbt.Keys.version

lazy val p1 = (project in file("."))
  .settings(
    name := "table-reader",
    version := "0.1",
    scalaVersion := "2.11.8",

    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "3.1.3",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.apache.spark" %% "spark-core" % "2.2.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided,
      "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.12.1" % Provided,
      "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.12.1" % Provided,
      "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.12.1" % Provided
    )

  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)