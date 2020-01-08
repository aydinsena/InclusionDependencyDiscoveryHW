package de.hu.sena_homework

import java.io.File

import com.beust.jcommander.Parameter
import com.beust.jcommander.JCommander


object Main {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val t = t1 - t0
    println("Elapsed time: " + t + "ns (" + t / 1000000000 + "s)")
    result
  }

  def getListOfFiles(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getAbsolutePath)
    } else {
      List()
    }
  }

  class Args {
    @Parameter(names = Array("--cores"), description = "Number of cores")
    var cores: Int = 4
    @Parameter(names = Array("--path"), description = "Path containing csv files")
    var path: String = "TPCH"
    @Parameter(names = Array("--master"))
    var master: String = "local"
  }

  def main(args: Array[String]): Unit = {
    val jcArgs = new Args()
    val jc = new JCommander()
    jc.addObject(jcArgs)
    jc.parse(args: _*)

    val inputs = getListOfFiles(jcArgs.path)

    time {
      new Sindy(jcArgs.master, jcArgs.cores).discoverINDs(inputs)
    }
  }

}
