
import java.io.File
import java.util.Base64

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}


case class badEvent(error: Seq[String], info: String)

object search_bad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("s3")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = spark.read.json("E:\\eks\\vomkt-evt\\enrich-bad\\2019\\07\\29\\04\\*.gz")

    data.show(1000, false)

    def writeToCSV(data: DataFrame): Unit = {
      val savePath = "d:/result"
      FileUtils.deleteDirectory(new File(savePath))
      data
        .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .option("header", "true")
        .option("delimiter", ",")
        .csv(savePath)
    }

    def decodeF(line: String): String = {
      val decodeLine = new String(Base64.getDecoder.decode(line), "UTF-8")
      val patternCx = "\"cx\":\"(\\w+)\"".r
      val patternUePx = "\"ue_px\":\"(\\w+)\"".r
      val cxRaw = patternCx.findFirstIn(decodeLine).getOrElse("")
      val cx = if (cxRaw.length > 6) cxRaw.substring(6, cxRaw.length - 1) else ""
      val uePxRaw = patternUePx.findFirstIn(decodeLine).getOrElse("")
      val uePx = if (uePxRaw.length > 9) uePxRaw.substring(9, uePxRaw.length - 1) else ""
      try {
        val result1 = if (cx.length > 0) new String(Base64.getUrlDecoder.decode(cx)) else ""
        val result2 = if (uePx.length > 0) new String(Base64.getUrlDecoder.decode(uePx)) else ""
        result1 + "\n" + result2
      } catch {
        case e: Exception => uePxRaw + "\n" + cxRaw
      }
    }


    val decode = spark.udf.register("decode_special", decodeF(_: String): String)
    val data2 = data
      .withColumn("decode_line", decode(functions.col("line")))
      //.withColumn("errors_string", arrayToString(functions.col("errors")))
      .select(functions.explode($"errors.message").alias("error_info"), $"decode_line")
      .cache()

    data2
      .select("error_info")
      .show(1, false)
    val savePath = "d:/result"
    FileUtils.deleteDirectory(new File(savePath))
    data2
      .coalesce(1)
      .write
      .json(savePath)
    spark.stop()
  }
}

object testBase64 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("s3")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = spark.read.textFile("E:\\eks\\vomkt-evt\\enrich-good\\2019\\07\\29\\04\\*.gz")

    data.show(1000, false)

  }
}
