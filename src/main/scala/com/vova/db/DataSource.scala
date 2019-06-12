package com.vova.db

import java.sql.{Connection, DriverManager}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class DataSource(configPath: String) {
  private lazy val root = DataSource.config.getConfig(configPath)

  private def configMap: Map[String, String] = {
    Map(
      "url" -> root.getString("url"),
      "user" -> root.getString("user"),
      "password" -> root.getString("password"),
      "driver" -> root.getString("driver")
    )
  }

  def load(spark: SparkSession, sqlStmt: String): DataFrame = {
    spark.read.format("jdbc")
      .options(configMap)
      .option("dbTable", s"($sqlStmt) t1")
      .load()
  }

  def overwriteTable(tableName: String, dataSet: Dataset[_]): Unit = {
    dataSet
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .options(configMap)
      .option("dbTable", tableName)
      .save()
  }

  def appendTable(tableName: String, dataSet: Dataset[_]): Unit = {
    dataSet
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .options(configMap)
      .option("dbTable", tableName)
      .save()
  }

  def insert(typeMap: Map[String, String], table: String, data: DataFrame) = {
    val fieldsArr = data.schema.map(f => f.name)
    val fields = data.schema.foldLeft("")((acc, f) => if (acc == "") f.name else acc + "," + f.name)
    val placeHolder = data.schema.foldLeft("")((acc, _) => if (acc == "") "?" else acc + "," + "?")
    val insertStart = s"REPLACE INTO $table ($fields) VALUES(" + placeHolder + ")"
    //防止 Task not serializable
    val (url, user, password) = (configMap.getOrElse("url", ""), configMap.getOrElse("user", ""), configMap.getOrElse("password", ""))
    //data.show(truncate = false)
    data.repartition(4).foreachPartition(t => {
      val connection: Connection = DriverManager.getConnection(url, user, password)
      val preparedStatement = connection.prepareStatement(insertStart)
      try {
        while (t.hasNext) {
          val row = t.next()
          fieldsArr.zipWithIndex.foreach(r => typeMap.getOrElse(r._1, "") match {
            case "string" => preparedStatement.setString(r._2 + 1, row.getAs[String](r._1))
            case "int" => preparedStatement.setInt(r._2 + 1, row.getAs[Int](r._1))
            case "long" => preparedStatement.setLong(r._2 + 1, row.getAs[Long](r._1))
            case "decimal" => preparedStatement.setBigDecimal(r._2 + 1, row.getAs[java.math.BigDecimal](r._1))
          })
          preparedStatement.addBatch()
        }
        preparedStatement.executeLargeBatch()
        preparedStatement.clearParameters()
      } catch {
        case e: Exception => {
          println("insert error")
          e.printStackTrace()
        }
      } finally {
        preparedStatement.close()
        connection.close()
      }
    })
  }

  def insertPure(table: String, data: DataFrame, spark: SparkSession) = {
    val fieldsArr = data.schema.map(f => (f.name, f.dataType.simpleString)).reverse
    val fields = data.schema.foldLeft("")((acc, f) => if (acc == "") f.name else acc + "," + f.name)
    val insertData = s"REPLACE INTO $table ($fields) VALUES"
    val (url, username, password) = (configMap.getOrElse("url", ""), configMap.getOrElse("user", ""), configMap.getOrElse("password", ""))
    data.foreachPartition(t => {
      var lines = ""
      while (t.hasNext) {
        val row = t.next()
        val line = fieldsArr.foldRight("")((key, acc) => {
          val f = if (key._2 == "string") {
            "'" + row.getAs[String](key._1).replace("'", "\\'") + "'"
          } else {
            "'" + row.getAs[String](key._1) + "'"
          }
          if (acc == "") f
          else acc + "," + f
        })
        lines = lines + (if (lines == "") "" else ",") + "(" + line + ")"
      }
      if (lines != "") {
        val connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()
        val insertSql = insertData + lines
        try {
          val status = statement.executeUpdate(insertSql)
          //println(s"insert:$status")
        } catch {
          case e: Exception => {
            println("insert error")
            println(insertSql)
            e.printStackTrace()
          }
        } finally {
          statement.close()
          connection.close()
        }
      }
    })
  }

  def execute(sql: String): Unit = {
    val (url, username, password) = (configMap.getOrElse("url", ""), configMap.getOrElse("user", ""), configMap.getOrElse("password", ""))
    val connection: Connection  = DriverManager.getConnection(url, username, password)
    val statement = connection.prepareStatement(sql)
    try {
      val status = statement.executeUpdate()
      //println(status)
    } catch {
      case e: Exception => {
        println(sql)
        e.printStackTrace()
      }
    } finally {
      statement.close()
      connection.close()
    }
  }

}

object DataSource {
  private lazy val buildEnv: String = System.getenv("BUILD_ENV")

  private def config: Config = buildEnv match {
    case "local" => ConfigFactory.load("local.conf")
    case _ => ConfigFactory.load("prod.conf")
  }
}
