package org.oclc.hbase.sparkdemo

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

class SimpleConnect extends App {
  val con = ConnectionFactory.createConnection()
  val admin = con.getAdmin()
  val spaces =  admin.listNamespaceDescriptors()
  println(s"we found ${spaces.size} descriptors")
  admin.close()
  con.close()

}
