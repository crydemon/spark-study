package ml_spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Try}


object ALSTest {
  val base = "D:\\work\\report-v1\\src\\main\\resources\\ml_data\\profiledata_06-May-2005\\"

  def main(args: Array[String]): Unit = {
    val spark = utils.util.initSpark("recommender")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")
    val als = new ALSTest(spark)
    //als.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    //als.model(rawUserArtistData, rawArtistData, rawArtistAlias)
    als.evaluate(rawUserArtistData, rawArtistAlias)
  }
}

class ALSTest(private val spark: SparkSession) {

  import spark.implicits._

  def preparation(
                   rawUserArtistData: Dataset[String],
                   rawArtistData: Dataset[String],
                   rawArtistAlias: Dataset[String]
                 ): Unit = {
    //look 一下
    rawUserArtistData.take(5).foreach(println)
    val userArtistDF = rawUserArtistData.map(line => {
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }).toDF("user", "artist")
    //查看数据情况
    userArtistDF.describe("user", "artist").show(false)
    //数据load
    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)
    val (badID, goodID) = artistAlias.head
    artistByID.filter($"id".isin(badID, goodID)).show()
  }

  def evaluate(rawUserArtistData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {
    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

    val allData = buildCounts(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
    val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

    val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
    println(mostListenedAUC)

    val rank = 40
    val regParam = 0.01
    val alpha = 1
    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(rank).setRegParam(regParam).
      setAlpha(alpha).setMaxIter(20).
      setUserCol("user").setItemCol("artist").
      setRatingCol("count").setPredictionCol("prediction").
      fit(trainData)

    val auc = areaUnderCurve(cvData, bAllArtistIDs, model.transform)
    println(auc)
    trainData.unpersist()
    cvData.unpersist()
  }

  def buildArtistByID(rawArtistData: Dataset[String]): DataFrame = {
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      Try(Some(id.toInt, name.trim)).getOrElse(None)
    }.toDF("id", "name")
  }

  def buildArtistAlias(rawArtistAlias: Dataset[String]): Map[Int, Int] = {
    rawArtistAlias.flatMap { line =>
      val Array(artist, alias) = line.split('\t')
      if (artist.isEmpty) None else Some(artist.toInt, alias.toInt)
    }.collect().toMap
  }

  def model(rawUserArtistData: Dataset[String], rawArtistData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {
    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
    val trainData = buildCounts(rawUserArtistData, bArtistAlias)
    val model = new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      //用户-特征，产品-特征，矩阵的阶
      .setRank(10)
      //正则化，防止过拟合
      .setRegParam(0.01)
      //用户-产品交互时相对没有被观察到的交互权重
      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(trainData)
    trainData.unpersist()

    model.userFactors.select("features").show(truncate = false)

    val userID = 2093760

    val existingArtistIDs = trainData.
      filter($"user" === userID).
      select("artist").as[Int].collect()

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter($"id" isin (existingArtistIDs: _*)).show()

    val topRecommendations = makeRecommendations(model, userID, 5)
    topRecommendations.show()

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()

    artistByID.filter($"id" isin (recommendedArtistIDs: _*)).show()

    model.userFactors.unpersist()
    model.itemFactors.unpersist()

  }

  def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
    //a DataFrame that stores item factors in two columns: id and features
    val toRecopmmend = model.itemFactors.select($"id".as("artist"))
      .withColumn("user", lit(userID))
    model.transform(toRecopmmend)
      .select("artist", "prediction")
      .orderBy($"prediction".desc)
      .limit(howMany)
  }

  def buildCounts(rawUserArtistData: Dataset[String], bArtistAlias: Broadcast[Map[Int, Int]]): DataFrame = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      //如果有别名取别名
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      (userID, finalArtistID, count)
    }.toDF("user", "artist", "count")

  }


  //求平均auc
  def areaUnderCurve(positiveData: DataFrame, bAllArtistIDs: Broadcast[Array[Int]], predictFunction: (DataFrame => DataFrame)): Double = {
    val negativeData = positiveData.select("user", "artist").as[(Int, Int)]
      .groupByKey { case (user, _) => user }
      .flatMapGroups { case (userID, userIDAndPosArtistIDs) => {
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
        val negative = new ArrayBuffer[Int]()
        val allArtistIDs = bAllArtistIDs.value
        var i = 0
        while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
          val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
          if (!posItemIDSet.contains(artistID)) {
            negative += 1
          }
          i += 1
        }
        negative.map(excludeArtistIDs => (userID, excludeArtistIDs))
      }
      }.toDF("user", "artist")

    //在正样本上预测一下
    val positivePredictions = predictFunction(positiveData.select("user", "artist")).
      withColumnRenamed("prediction", "positivePrediction")
    //在负样本上预测一下
    val negativePrediction = predictFunction(negativeData)
      .withColumnRenamed("prediction", "negativePrediction")
    val joinedPrediction = positivePredictions.join(negativePrediction, "user")
      .select("user", "positivePrediction", "negativePrediction")
    joinedPrediction.groupBy("user").agg(
      sum(lit(1)).as("total"),
      sum(when($"positivePrediction" > $"negativePrediction", 1).otherwise(0)).as("correct")
    ).select("user", "total", "correct")
      .withColumn("auc", $"correct" / $"total")
      .agg(mean("auc"))
      .as[Double]
      .first()
  }


  //将播放最多推荐给用户
  def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts = train.groupBy("artist").
      agg(sum("count").as("prediction")).
      select("artist", "prediction")
    allData.
      join(listenCounts, Seq("artist"), "left_outer").
      select("user", "artist", "prediction")
  }

}

