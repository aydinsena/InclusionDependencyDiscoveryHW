package de.hu.sena_homework

import org.apache.spark
import SparkJob.spark.implicits._
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Sindy extends SparkJob {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    val dataFrames: List[DataFrame] = inputs.map(f => {
      spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("delimiter", ";")
        .load(f)
    }
    )

    val cells: Dataset[Cells] = dataFrames
      .map(dataFrameToCells)
      .reduce(_.unionAll(_))

    val attributeSet = cellsToAttributeSet(cells)
    val inclusionLists = toInclusionLists(attributeSet)
    val inclusionDependencies = finalAggregate(inclusionLists)


    val printINDs = inclusionDependencies.collect()
      .map(x => {
        s"${x.attributeValues} < ${x.attributeNames.mkString(", ")}"
      }).sorted
      .mkString("\n")
    println("RESULT: " + "\n" + printINDs)
  }


  def dataFrameToCells(df: DataFrame): Dataset[Cells] = {
    val columnNames = df.columns.map(Set(_))
    df.rdd.flatMap(row => {
      row
        .toSeq
        .map(_.toString)
        .zip(columnNames) // merge with column names
        .map { case (attributeValues, attributeNames) =>
          Cells(attributeValues, attributeNames)
        }
    }).toDS
  }

  def cellsToAttributeSet(cells: Dataset[Cells]): Dataset[Set[String]] = {
    cells.groupByKey(_.attributeValues).mapGroups((_, iter) => {
      iter.map(_.attributeNames).reduce(_.union(_))
    })
  }

  def toInclusionLists(attributeSet: Dataset[Set[String]]): Dataset[Cells] = {
    attributeSet.flatMap(x => {
      x.map(y => {
        Cells(y, x.filterNot(_.equals(y)))
      })
    })
  }

  def finalAggregate(inclusionLists: Dataset[Cells]): Dataset[Cells] = {
    inclusionLists
      .groupByKey(_.attributeValues)
      .mapGroups((k, v) => {
        Cells(k, v.map(_.attributeNames).reduce(_.intersect(_)))
      })
      .filter(x => x.attributeNames.nonEmpty)
  }

}