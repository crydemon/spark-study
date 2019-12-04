package ml_spark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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
    println("---------------------random classifier------------------")
    rdf.randomClassifier(trainData, testData)
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
      as[(Double, Double)].rdd
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

  def classProbabilities(data: DataFrame): Array[Double] = {
    val total = data.count()
    data.groupBy("Cover_Type").count().
      orderBy("Cover_Type").
      select("count").as[Double].
      map(_ / total).
      collect()
  }

  def randomClassifier(trainData: DataFrame, testData: DataFrame): Unit = {
    val trainPriorProbabilities = classProbabilities(trainData)
    val testPriorProbabilities = classProbabilities(testData)
    val accuracy = trainPriorProbabilities.zip(testPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    println(accuracy)
  }

  def evaluate(trainData: DataFrame, testData: DataFrame): Unit = {

    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")
    val pipeline = new Pipeline().setStages(Array(assembler, classifier))

    //ParamGridBuilder 构建参数网格
    val paramGrid = new ParamGridBuilder().
    //增益选项
      addGrid(classifier.impurity, Seq("gini", "entropy")).
    //树的最大深度
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()
    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(trainData)

    val paramsAndMetrics = validatorModel.validationMetrics.
      zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

    paramsAndMetrics.foreach { case (metric, params) =>
      println(metric)
      println(params)
      println()
    }
    val bestModel = validatorModel.bestModel

    println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    println(validatorModel.validationMetrics.max)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
    println(testAccuracy)

    val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
    println(trainAccuracy)
  }

  def unencodeOneHot(data: DataFrame): DataFrame = {
    val wildernessCols = (0 until 4).map(i => s"Wilderness_Area_$i").toArray

    val wildernessAssembler = new VectorAssembler().
      setInputCols(wildernessCols).
      setOutputCol("wilderness")

    val unhotUDF = udf((vec: DenseVector) => vec.toArray.indexOf(1.0).toDouble)

    val withWilderness = wildernessAssembler.transform(data).
      drop(wildernessCols: _*).
      withColumn("wilderness", unhotUDF($"wilderness"))

    val soilCols = (0 until 40).map(i => s"Soil_Type_$i").toArray

    val soilAssembler = new VectorAssembler().
      setInputCols(soilCols).
      setOutputCol("soil")

    soilAssembler.transform(withWilderness).
      drop(soilCols: _*).
      withColumn("soil", unhotUDF($"soil"))
  }

  def evaluateCategorical(trainData: DataFrame, testData: DataFrame): Unit = {
    val unencTrainData = unencodeOneHot(trainData)
    val unencTestData = unencodeOneHot(testData)

    val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featureVector").
      setOutputCol("indexedVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("indexedVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(unencTrainData)

    val bestModel = validatorModel.bestModel

    println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
    println(testAccuracy)
  }

  def evaluateForest(trainData: DataFrame, testData: DataFrame): Unit = {
    val unencTrainData = unencodeOneHot(trainData)

    val unencTestData = unencodeOneHot(testData)
    val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featureVector").
      setOutputCol("indexedVector")

    val classifier = new RandomForestClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("indexedVector").
      setPredictionCol("prediction").
      setImpurity("entropy").
      setMaxDepth(20).
      setMaxBins(300)

    val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      addGrid(classifier.numTrees, Seq(1, 10)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(unencTrainData)

    val bestModel = validatorModel.bestModel

    val forestModel = bestModel.asInstanceOf[PipelineModel].
      stages.last.asInstanceOf[RandomForestClassificationModel]

    println(forestModel.extractParamMap)
    println(forestModel.getNumTrees)
    forestModel.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(println)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
    println(testAccuracy)

    bestModel.transform(unencTestData.drop("Cover_Type")).select("prediction").show()
  }

}
