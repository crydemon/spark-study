package thinks.spark

object DirtyWork extends App {
  val spark = utils.util.initSpark("think_encoder")

  import spark.implicits._

  val csv = spark.read
    .option("header", "true")
    .csv("C:\\Users\\admin\\Documents\\WeChat Files\\kant132\\FileStorage\\File\\2020-01\\it.csv")
  csv.show(200)
}
