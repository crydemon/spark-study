package ml_spark.datastatis


object Correlation extends App {
  import org.apache.spark.ml.linalg.{Matrix, Vectors}
  import org.apache.spark.ml.stat.{Correlation => corr}
  import org.apache.spark.sql.Row
  val spark = utils.util.initSpark("OneHot")
  import spark.implicits._
  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
  )

  val df = data.map(Tuple1.apply).toDF("features")
  val Row(coeff1: Matrix) =corr.corr(df, "features").head
  println(s"Pearson correlation matrix:\n $coeff1")

  val Row(coeff2: Matrix) = corr.corr(df, "features", "spearman").head
  println(s"Spearman correlation matrix:\n $coeff2")
}
//卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，卡方值越大，越不符合
object HypothesisTesting  extends App {
  import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
  import org.apache.spark.ml.stat.ChiSquareTest
  val spark = utils.util.initSpark("OneHot")
  import spark.implicits._

  val data = Seq(
    (0.0, Vectors.dense(0.5, 10.0)),
    (0.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (0.0, Vectors.dense(3.5, 30.0)),
    (0.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
  )

  val df = data.toDF("label", "features")
  val chi = ChiSquareTest.test(df, "features", "label").head
  println(s"pValues = ${chi.getAs[Vector](0)}")
  println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
  println(s"statistics ${chi.getAs[Vector](2)}")
}

object Summarizer extends App {
  import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
  import org.apache.spark.ml.stat.ChiSquareTest
  import org.apache.spark.ml.stat.Summarizer._
  import org.apache.spark.sql.{functions => F}
  val spark = utils.util.initSpark("OneHot")
  import spark.implicits._
  val data = Seq(
    (Vectors.dense(2.0, 3.0, 5.0), 1.0),
    (Vectors.dense(4.0, 6.0, 7.0), 2.0)
  )

  val df = data.toDF("features", "weight")

  val (meanVal, varianceVal) = df.select(metrics("mean", "variance")
    .summary($"features", $"weight").as("summary"))
    .select("summary.mean", "summary.variance")
    .as[(Vector, Vector)].first()

  println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

  val (meanVal2, varianceVal2) = df.select(mean($"features"), variance($"features"))
    .as[(Vector, Vector)].first()

  println(s"without weight: mean = ${meanVal2}, sum = ${varianceVal2}")
}
