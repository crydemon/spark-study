
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

    val data = spark.read.json("E:\\eks\\vomkt-evt\\enrich-bad\\2019\\06\\28\\21")

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

object testBase64 extends App {
  val code = "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uZ29vZ2xlLmFuYWx5dGljcy9jb29raWVzL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7fX0seyJzY2hlbWEiOiJpZ2x1OmNvbS5hcnRlbWlzL2NvbW1vbi9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJsYW5ndWFnZSI6ImVuIiwiY291bnRyeSI6IkdCIiwiY3VycmVuY3kiOiJHQlAiLCJnZW5kZXIiOiIiLCJ1c2VyX3VuaXF1ZV9pZCI6LTEsInVzZXJfaWQiOiIiLCJkb21haW5fdXNlcl9pZCI6IiIsInBhZ2VfY29kZSI6Imdkbl9nb29kc19kZXRhaWwifX0seyJzY2hlbWEiOiJpZ2x1OmNvbS5hcnRlbWlzL2FwcF9jb21tb24vanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsiYXBwX3ZlcnNpb24iOiIiLCJkZXZpY2VfbW9kZWwiOiIiLCJyZWZlcnJlciI6Imh0dHBzOi8vd3d3Lmdvb2dsZS5jb20vIiwidXJpIjoiL2VuL2FkLXByb2R1Y3Q_Y3VycmVuY3k9R0JQJmNvdW50cnlfY29kZT1HQiZ1dG1fdGVybT1WT2NHQmxlbmc0MjMxODcyJmFfcz1nb29nbGUmYV9zPWdvb2dsZSZhX209cGxhJmFfbT1jcGMmYV9mPWQmYV9mPWQmdmlydHVhbF9nb29kc19pZD01MTgwOTcxJmFfYz0yMDI5Mjc4NDMxLjY4NTc4MzUzOTgxJmFfdD0mYV9jdD0zNTUwMTIwNDgyMzkmY2hhbm5lbF90eXBlPXBsYSZnY2xpZD1DajBLQ1Fqd2k0M29CUkRCQVJJc0FFeFNSUUd4MHdFamNnTndBaUotQ2d2Mk1ycHVVZVFQcXVCbmo0aUc5ejhCQ0pHTjNBdmRCbDNWZHQ0YUFyUzZFQUx3X3djQiJ9fSx7InNjaGVtYSI6ImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L3dlYl9wYWdlL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7ImlkIjoiZjNiMzhjYjItZTg5MC00M2IwLThiMjAtOGE4MmRiOTgwODlhIn19LHsic2NoZW1hIjoiaWdsdTpvcmcudzMvUGVyZm9ybWFuY2VUaW1pbmcvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsibmF2aWdhdGlvblN0YXJ0IjoxNTYwNTI5NTY4MTcwLCJ1bmxvYWRFdmVudFN0YXJ0IjowLCJ1bmxvYWRFdmVudEVuZCI6MCwicmVkaXJlY3RTdGFydCI6MTU2MDUyOTU2ODE4NCwicmVkaXJlY3RFbmQiOjE1NjA1Mjk1Njg3NDcsImZldGNoU3RhcnQiOjE1NjA1Mjk1Njg3NDcsImRvbWFpbkxvb2t1cFN0YXJ0IjoxNTYwNTI5NTY4NzQ3LCJkb21haW5Mb29rdXBFbmQiOjE1NjA1Mjk1Njg3NDcsImNvbm5lY3RTdGFydCI6MTU2MDUyOTU2ODc0NywiY29ubmVjdEVuZCI6MTU2MDUyOTU2ODc0Nywic2VjdXJlQ29ubmVjdGlvblN0YXJ0IjowLCJyZXF1ZXN0U3RhcnQiOjE1NjA1Mjk1Njg3NTgsInJlc3BvbnNlU3RhcnQiOjE1NjA1Mjk1Njk0OTYsInJlc3BvbnNlRW5kIjoxNTYwNTI5NTY5NTAzLCJkb21Mb2FkaW5nIjoxNTYwNTI5NTY5NjExLCJkb21JbnRlcmFjdGl2ZSI6MTU2MDUyOTU2OTY0MywiZG9tQ29udGVudExvYWRlZEV2ZW50U3RhcnQiOjE1NjA1Mjk1NzA3NDcsImRvbUNvbnRlbnRMb2FkZWRFdmVudEVuZCI6MTU2MDUyOTU3MDc0NywiZG9tQ29tcGxldGUiOjE1NjA1Mjk1NzE1NzIsImxvYWRFdmVudFN0YXJ0IjoxNTYwNTI5NTcxNTcyLCJsb2FkRXZlbnRFbmQiOjE1NjA1Mjk1NzE1NzMsImNocm9tZUZpcnN0UGFpbnQiOjE1NjA1Mjk1Njk2ODN9fV19"
  println(code.length % 3)
  println(new String(Base64.getUrlDecoder.decode(code)))
}
