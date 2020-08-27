name := "snoop2"
organization := "org.oclc"
version := "0.1"
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  ("org.apache.hbase" % "hbase-client" % "1.2.0" % Compile)
    .exclude("commons-beanutils","commons-beanutils")
    .exclude("commons-beanutils","commons-beanutils-core")
    .exclude("org.apache.hadoop", "hadoop-yarn-common")
//    .exclude("commons-collection", "commons-collections")
  ,
  ("org.scalatest" %% "scalatest" % "3.2.0" % Test)
//    .exclude("commons-beanutils","commons-beanutils")
//    .exclude("commons-beanutils","commons-beanutils-core")
//    .exclude("commons-collection", "commons-collections")

)