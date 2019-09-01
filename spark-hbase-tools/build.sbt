import sbt.Keys.version

lazy val p1 = (project in file("."))
  .settings(
    name := "spark-hbase-tools",
    version := "0.1",
    scalaVersion := "2.11.8",

    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "3.1.3",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.apache.spark" %% "spark-core" % "2.2.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided,
      "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.12.1" % Provided,
      "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.12.1" % Provided,
      "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.12.1" % Provided,
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5" % Compile
    )

  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)