package com.vova.export

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Utils {




  def getFieldsList(data: DataFrame): List[String] = data.schema.map(f => f.name).reverse.toList

  def getFields(data: DataFrame): String = data.schema.foldLeft("")((acc, f) => if (acc == "") f.name else acc + "," + f.name)

  def getOneRow(row: Row, fieldsList: List[String]): String = fieldsList.foldRight("")((key, acc) => acc + (if (acc == "") "" else ",") + row.getAs[String](key))

  def writeToCSV(fileName: String, data: DataFrame, spark: SparkSession): Unit = {
    val fieldsList = getFieldsList(data)
    val file = new File(s"d:/$fileName")
    if (!file.exists()) {
      val fields = getFields(data)
      FileUtils.write(file, fields + "\n", "utf-8", true)
    }
    data.foreach(row => {
      val line = getOneRow(row, fieldsList)
      FileUtils.write(file, line + "\n", "utf-8", true)
    })
  }

  def writeToCSVV2(fileName: String, data: DataFrame, spark: SparkSession): Unit = {
    val fieldsList = getFieldsList(data)
    val file = new File(s"d:/$fileName.csv")
    if (!file.exists()) {
      val fields = getFields(data)
      FileUtils.write(file, fields + "\n", "utf-8", true)
    }
    data.foreachPartition(t => {
      val list = new java.util.ArrayList[String]()
      while (t.hasNext) {
        val row = t.next()
        val line = getOneRow(row, fieldsList)
        list.add(line)
      }
      val content = list.toArray.foldLeft("")(_ + _ + "\n")
      FileUtils.write(file, content, "utf-8", true)
    })
  }

}


//import org.apache.spark.util.CollectionAccumulator
//val rowsCollector:CollectionAccumulator[String] = spark.sparkContext.collectionAccumulator("rows")
//val content = rowsCollector.value.toArray.foldLeft("")(_ + _ + "\n")
//FileUtils.write(file, content, "utf-8", true)
//rowsCollector.add(line)
