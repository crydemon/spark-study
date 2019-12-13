
package ml_spark

// $example on$
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example for computing correlation matrix.
 * Run with
 * {{{
 * bin/run-example ml.CorrelationExample
 * }}}
 */
object CorrelationExample {

  def main(args: Array[String]): Unit = {
    println(70934149 % 16)

    val spark = utils.util.initSpark("rdf")
    import spark.implicits._

    // $example on$
    val data = Seq(
      //稀疏矩阵
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))).toDense,
      //稠密矩阵
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(3.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    for (elem <- data) {println(elem)}

    println(data.map(Tuple1.apply).foreach(println))
    val df = data.map(Tuple1.apply).toDF("features")
    df.show(false)
    val corr = Correlation.corr(df, "features")
    println(corr.count())
    corr.show(false)
    val Row(coeff1: Matrix) = corr.head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
