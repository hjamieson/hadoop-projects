package org.oclc.hbase.snoop2.impl

case class Observation(val table: String, val rowKey:Array[Byte], val createMs: Long, val elapsedMs:Long )
