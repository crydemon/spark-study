package spark

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, EqualTo, Filter, GreaterThan, PrunedFilteredScan, PrunedScan, RelationProvider, SchemaRelationProvider, TableScan}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new TextDataSourceRelation(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-datasource format!!")
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", "./output/") //can throw an exception/error, it's just for this tutorial
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    mode match {
      case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists => sys.error("Given path: " + path + " already exists!!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }

    val formatName = parameters.getOrElse("format", "customFormat")
    formatName match {
      case "customFormat" => saveAsCustomFormat(data, path, mode)
      case "json" => saveAsJson(data, path, mode)
      case _ => throw new IllegalArgumentException(formatName + " is not supported!!!")
    }
    createRelation(sqlContext, parameters, data.schema)
  }

  private def saveAsJson(data: DataFrame, path: String, mode: SaveMode): Unit = {
    /**
      * Here, I am using the dataframe's Api for storing it as json.
      * you can have your own apis and ways for saving!!
      */
    data.write.mode(mode).json(path)
  }

  private def saveAsCustomFormat(data: DataFrame, path: String, mode: SaveMode): Unit = {
    /**
      * Here, I am  going to save this as simple text file which has values separated by "|".
      * But you can have your own way to store without any restriction.
      */
    val customFormatRDD = data.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString("|")
    })
    customFormatRDD.saveAsTextFile(path)
  }
}

class TextDataSourceRelation(override val sqlContext: SQLContext, path: String, userSchema: StructType)
  extends BaseRelation
    with Serializable
    with TableScan
    with PrunedScan
    with PrunedFilteredScan {
  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("id", StringType, false) ::
          StructField("name", StringType, false) ::
          StructField("gender", StringType, false) ::
          StructField("salary", LongType, false) ::
          StructField("expenses", LongType, false) :: Nil)
    }
  }

  override def buildScan(): RDD[Row] = {
    println("TableScan: buildScan called...")
    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f =>  f._2)

    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map {
        case (value, index) =>
          val colName = schemaFields(index).name
          Util.castTo(
            if (colName.equalsIgnoreCase("gender")) {
              if (value.toInt == 1) {
                "Male"
              } else {
                "Female"
              }
            } else {
              value
            }, schemaFields(index).dataType)
      })
      tmp.map(s => Row.fromSeq(s))
    })
    rows.flatMap(e => e)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    println("PrunedScan: buildScan called...")

    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map {
        case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value.toInt == 1) "Male" else "Female"
          } else value,
            schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
      })

      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })

    rows.flatMap(e => e)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("PrunedFilterScan: buildScan called...")

    println("Filters: ")
    filters.foreach(f => println(f.toString))

    var customFilters: Map[String, List[CustomFilter]] = Map[String, List[CustomFilter]]()
    filters.foreach(f => f match {
      case EqualTo(attr, value) =>
        println("EqualTo filter is used!!" + "Attribute: " + attr + " Value: " + value)

        /**
          * as we are implementing only one filter for now, you can think that this below line doesn't mak emuch sense
          * because any attribute can be equal to one value at a time. so what's the purpose of storing the same filter
          * again if there are.
          * but it will be useful when we have more than one filter on the same attribute. Take the below condition
          * for example:
          * attr > 5 && attr < 10
          * so for such cases, it's better to keep a list.
          * you can add some more filters in this code and try them. Here, we are implementing only equalTo filter
          * for understanding of this concept.
          */
        customFilters = customFilters ++ Map(attr -> {
          customFilters.getOrElse(attr, List[CustomFilter]()) :+ new CustomFilter(attr, value, "equalTo")
        })
      case GreaterThan(attr, value) =>
        println("GreaterThan Filter is used!!" + "Attribute: " + attr + " Value: " + value)
        customFilters = customFilters ++ Map(attr -> {
          customFilters.getOrElse(attr, List[CustomFilter]()) :+ new CustomFilter(attr, value, "greaterThan")
        })
      case _ => println("filter: " + f.toString + " is not implemented by us!!")
    })

    val schemaFields = schema.fields
    // Reading the file's content

    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(file => {
      val lines = file.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)

      val filteredData = data.map(s => if (customFilters.nonEmpty) {
        var includeInResultSet = true
        s.zipWithIndex.foreach {
          case (value, index) =>
            val attr = schemaFields(index).name
            val filtersList = customFilters.getOrElse(attr, List())
            if (filtersList.nonEmpty) {
              if (CustomFilter.applyFilters(filtersList, value, schema)) {
              } else {
                includeInResultSet = false
              }
            }
        }
        if (includeInResultSet) s else Seq()
      } else s)
      val tmp = filteredData.filter(_.nonEmpty).map(s => s.zipWithIndex.map {
        case (value, index) =>
          val field = schemaFields(index)
          val cast = field.name match {
            case x if x.equalsIgnoreCase("gender") => if(value.toInt == 1) "Male" else "Female"
            case _ => value
          }
          val castedValue = Util.castTo(cast, field.dataType)
          if (requiredColumns.contains(field.name)) Some(castedValue) else None
      })

      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })
    rows.flatMap(e => e)
  }
}

case class CustomFilter(attr: String, value: Any, filter: String)

object CustomFilter {
  def applyFilters(filters: List[CustomFilter], value: String, schema: StructType): Boolean = {
    var includeInResultSet = true

    val schemaFields = schema.fields
    val index = schema.fieldIndex(filters.head.attr)
    val dataType = schemaFields(index).dataType
    val castedValue = Util.castTo(value, dataType)

    filters.foreach(f => {
      val givenValue = Util.castTo(f.value.toString, dataType)
      f.filter match {
        case "equalTo" => {
          includeInResultSet = castedValue == givenValue
          println("custom equalTo filter is used!!")
        }
        case "greaterThan" => {
          includeInResultSet = castedValue.equals(givenValue)
          println("custom greaterThan filter is used!!")
        }
        case _ => throw new UnsupportedOperationException("this filter is not supported!!")
      }
    })

    includeInResultSet
  }
}

object Util {
  def castTo(value: String, dataType: DataType) = {
    dataType match {
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }
}

object TestApp extends App {
  println("Application Started...")
  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  import spark.implicits._
  val df = spark.sqlContext.read.format("spark.DefaultSource").load("D:\\datas\\users")
  //println("output schema...")
  //df.printSchema()
  df.foreach(r => println(r.toSeq))

  df.map(r=> r.get(0).toString).toDF("id").show(20)
  //wrong ???
  df.show()
  df.createOrReplaceTempView("users")
  spark.sql(
    """
      |select id, name from users where id >= 10002
      |""".stripMargin)
      .show()
  //save the data
//  df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("spark.DefaultSource").save("d://Users//Downloads/out_custom/")
//  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("spark.DefaultSource").save("d://Users//Downloads/out_json/")
//  df.write.mode(SaveMode.Overwrite).format("spark.DefaultSource").save("d://Users//Downloads/out_none/")
  println("Application Ended...")
}

