package de.hu.sena_homework

import org.apache.spark.sql.{DataFrame, Dataset}

case class Cell(attributeValues : String, attributeNames:  Set[String])

class Sindy(override val master: String = "local", override val numCPUs: Int = 4) extends SparkJob with Serializable {

  import spark.implicits._

  def discoverINDs(inputs: List[String]): Unit = {

    val dataFrames: List[DataFrame] = inputs.map(path => {
      spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("delimiter", ";")
        .load(path)
    })

    val inclusionDependencies = finalAggregate(
      toInclusionLists(
        cellsToAttributeSet(dataFrames.map(
          dataFrameToCells).reduce(_.unionAll(_)))))

    val printINDs = inclusionDependencies.collect()
      .map(x => {
        s"${x.attributeValues} < ${x.attributeNames.mkString(", ")}"
      }).sorted
      .mkString("\n")
    println("RESULT: " + "\n" + printINDs)
  }

  def dataFrameToCells(df: DataFrame): Dataset[Cell] = {
    val columnNames = df.columns
    df.rdd.flatMap(row => {
      row
        .toSeq
        .zip(columnNames) // merge with column names
        .map { case (attributeValues, attributeNames) =>
          Cell(attributeValues.toString, Set(attributeNames))
        }
    }).toDS
  }

  def cellsToAttributeSet(cells: Dataset[Cell]): Dataset[Set[String]] = {
    cells.groupByKey(_.attributeValues).mapGroups((_, iter) => {
      iter.map(_.attributeNames).reduce(_.union(_))
    })
  }

  def toInclusionLists(attributeSet: Dataset[Set[String]]): Dataset[Cell] = {
    attributeSet.flatMap(x => {
      x.map(y => {
        Cell(y, x.filterNot(_.equals(y)))
      })
    })
  }

  def finalAggregate(inclusionLists: Dataset[Cell]): Dataset[Cell] = {
    inclusionLists
      .groupByKey(_.attributeValues)
      .mapGroups((k, v) => {
        Cell(k, v.map(_.attributeNames).reduce(_.intersect(_)))
      })
      .filter(x => x.attributeNames.nonEmpty)
  }

}