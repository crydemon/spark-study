package ml_spark

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object ch4_rdf {
  val basePath = "D:\\datas\\"

  def main(args: Array[String]): Unit = {
    val spark = utils.util.initSpark("rdf")
    import spark.implicits._

    val dataWithoutHeader = spark.read.
      option("inferSchema", true).
      option("header", false).
      csv(basePath + "covtype.data")
    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")
      ) ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")
    val data = dataWithoutHeader.toDF(colNames: _*).
      withColumn("Cover_Type", $"Cover_Type".cast("double"))
    data.show()
    data.printSchema()
    data.describe()
    data.head
    val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
    val rdf = new RunRDF(spark)
    rdf.simpleDecisionTree(trainData, testData)
  }
}

class RunRDF(private val spark: SparkSession) {

  import spark.implicits._


  def simpleDecisionTree(trainData: DataFrame, testData: DataFrame): Unit = {
    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    //VectorAssembler是一个transformer，它可以将给定的多列转换成单个vector列。
    // 这对于将多个raw features、以及由不同的feature transformers生成的features，组合成单个feature vector，以便于训练ML model（比如logistic regression 和决策树）。
    //VectorAssembler接受下面的输入列类型：所有numeric类型，boolean类型，vector类型。在每行中，输入列的值都会被串联成一个指定顺序的vector。
    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("featureVector")
    val assembledTrainData = assembler.transform(trainData)

    assembledTrainData.select("featureVector").show(false)

    //定义分类器
    val classifier = new DecisionTreeClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("featureVector")
      .setPredictionCol("prediction")

    //训练模型
    val model = classifier.fit(assembledTrainData)
    //输出决策过程
    println(model.toDebugString)

    model.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(println)

    //进行预测
    val predictions = model.transform(assembledTrainData)


    predictions.select("Cover_Type", "prediction", "probability").
      show(truncate = false)

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val f1 = evaluator.setMetricName("f1").evaluate(predictions)
    println("accuracy: " + accuracy)
    println("f1: " + f1)

    val predictionRDD = predictions.
      select("prediction", "Cover_Type").
      as[(Double,Double)].rdd
    val multiClassMetrics = new MulticlassMetrics(predictionRDD)
    println(multiClassMetrics.confusionMatrix)

    println("---------------confusion matrix-----------")
    val confusionMatrix = predictions.
      groupBy("Cover_Type").
      pivot("prediction", (1 to 7)).
      count().
      na.fill(0.0).
      orderBy("Cover_Type")

    confusionMatrix.show()
  }

}
