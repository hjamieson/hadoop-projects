package org.oclc.hbase.snoop2.impl

import scala.collection.mutable

class SafeList[T] {
  val store = mutable.ListBuffer[T]()

  def add(t: T): Unit = synchronized {
    store += t
  }

  def +=(t: T) = add(t)

  def all() = synchronized {
    store.toList
  }

  def clear: Unit = synchronized {
    store.clear()
  }

  def size = store.size

}
