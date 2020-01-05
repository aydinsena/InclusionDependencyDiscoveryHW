package de.hu.sena_homework

import org.apache.spark.sql.SparkSession

//reduce the number of imports for import spark.implicits._
trait SparkJob {
    implicit val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
    .master("local[4]")
      .appName("Sindy")
      .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.shuffle.partitions", "8")
}

object SparkJob extends SparkJob {}
