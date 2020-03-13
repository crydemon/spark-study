package spark

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ThinkEncoder extends App {
  val spark = utils.util.initSpark("think_encoder")

  import spark.implicits._

  val s = Seq("33", "44", "55")
  val df = spark
    .createDataset(s)

  df.map(s => "hi," + s).show(false)
  df.show(false)
  val m1 = Map("a" -> 1)
  val m2 = Map("b" -> 10)
  val m3 = Map("c" -> 100)
  val seq1 = Seq(m1, m2, m3)
  val a1 = Array(1, 2, 3)
  val a2 = Array(3, 5)
  val a3 = Array(1, 2, 3)
  val seq2 = Seq(a1, a2, a3)
  val s1 = "abc"
  val s2 = "abc2"
  val s3 = "abc3"
  val seq3 = Seq(s1, s2, s3)

  import org.apache.spark.sql.Row

  val r1 = Row(1, "a")
  val r2 = Row(2, "b")
  val r3 = Row(3, "c")
  val seq4 = Seq(r1, r2, r3)

  val seqt = seq1.zip(seq2).zipWithIndex.map { case (v, idx) => (v._1, v._2, seq3(idx)) }
  val seq = seqt.zipWithIndex.map { case (v, idx) => (v._1, v._2, v._3, seq4(idx)) }
  val encoder = Encoders.tuple(
    newMapEncoder[Map[String, Int]],
    newIntArrayEncoder,
    Encoders.STRING,
    RowEncoder(
      StructType(
        Seq(
          StructField("num", IntegerType),
          StructField("str", StringType)
        )
      )
    )
  )
  spark.createDataset(seq)(encoder)
    .show(false)


}

case class People(name: String, age: Int)