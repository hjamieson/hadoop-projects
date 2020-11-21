package job

object Fibonacci {
  val fibs: LazyList[BigInt] =
    BigInt(0) #:: BigInt(1) #:: fibs.zip(fibs.tail).map( n => n._1 + n._2)

  println(fibs.take(5).toList)
}


