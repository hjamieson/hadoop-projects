package org.oclc.hbase.snoop2.impl

import org.scalatest.flatspec.AnyFlatSpec

class SafeListSpec extends AnyFlatSpec {
  val safeList = new SafeList[String]
  "a SafeList" should "allow adding elements" in {
    safeList.add("one")
    safeList.add("two")
    safeList.add("three")
  }

  it should "provide a size method" in {
    assert(safeList.size==3)
  }
  it should "return a list" in {
    assert(safeList.all().size == 3)
  }

  it should "allow list clear" in {
    safeList.clear
    assert(safeList.all().size == 0)
  }

  it should "allow += append" in {
    safeList += "able"
    safeList += "baker"
    assert(safeList.size == 2)
  }

  it should "be threadsafe" in {
    val nums = new SafeList[Int]
    val t1 = new Runnable {
      override def run() = {
        for (a <- 1 to 10) nums += 1
      }
    }
    for (a <- 1 to 3) {
      new Thread(t1).start
    }
    Thread.sleep(100)
    assert(nums.size== 30)
    assert(nums.all().reduce(_ + _) == 30)
  }
}
