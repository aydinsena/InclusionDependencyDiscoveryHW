package de.hu.sena_homework

import org.apache.spark.sql.SparkSession

//reduce the number of imports for import spark.implicits._
trait SparkJob {
  val master: String
  val numCPUs: Int

  implicit val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
    .master(s"$master[$numCPUs]")
    .appName("Sindy")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
}

