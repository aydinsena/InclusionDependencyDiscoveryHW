package de.hu.sena_homework

object Main extends SparkJob {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val t = t1 - t0
    println("Elapsed time: " + t + "ns (" + t / 1000000000 + "s)")
    result
  }

  def main(args: Array[String]): Unit = {

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders").map(name => s"data/TPCH/tpch_$name.csv")

    time {
      Sindy.discoverINDs(inputs, spark)
    }

  }

}
