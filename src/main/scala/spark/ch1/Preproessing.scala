package spark.ch1

import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{functions => F}

object Preproessing {
  def main(args: Array[String]): Unit = {
    //stringIndexerStages.foreach(s => s.transform(allData.select(s.getInputCol)).show(20))
    print(testData.count())
  }

  var trainSample = 1.0
  var testSample = 1.0
  val train = "E:\\datas\\allstate-claims-severity\\train.csv"
  val test = "E:\\datas\\allstate-claims-severity\\test.csv"

  val spark = SparkSessionCreate.createSession()
  println("Reading data from " + train + " file")

  val trainInput1 = spark.read
    .option("header", "true")
    .csv(train)
    .cache

  val trainInput = trainInput1.select(trainInput1.columns.filterNot(isCateg).map(c => F.col(c).cast(DoubleType)):_*)
  val testInput1 = spark.read
    .option("header", "true")
    .csv(test)
    .cache
  val testInput = testInput1.select(testInput1.columns.filterNot(isCateg).map(c => F.col(c).cast(DoubleType)):_*)

  println("Preparing data for training model")
  var data = trainInput.withColumnRenamed("loss", "label").sample(false, trainSample)
  var DF = data.na.drop()

  // Null check
  if (data == DF)
    println("No null values in the DataFrame")
  else {
    println("Null values exist in the DataFrame")
    data = DF
  }

  val seed = 12345L
  val splits = data.randomSplit(Array(0.75, 0.25), seed)
  val (trainingData, validationData) = (splits(0), splits(1))

  trainingData.cache
  validationData.cache

  val testData = testInput.na.fill(0)

  def isCateg(c: String): Boolean = c.startsWith("cat")

  def categNewCol(c: String): String = if (isCateg(c)) s"idx_${c}" else c
  def categNewCol1(c: String): String = if (isCateg(c)) s"idx1_${c}" else c

  // Function to remove categorical columns with too many categories
  def removeTooManyCategs(c: String): Boolean = !(c matches "cat(109$|110$|112$|113$|116$)")

  // Function to select only feature columns (omit id and label)
  def onlyFeatureCols(c: String): Boolean = !(c matches "id|label")

  // Definitive set of feature columns
  val featureCols = trainingData.columns
    .filter(removeTooManyCategs)
    .filter(onlyFeatureCols)
    .map(categNewCol)

  // StringIndexer for categorical columns (OneHotEncoder should be evaluated as well)
  val allData = trainInput.drop("loss").union(testInput)
  val stringIndexerStages = trainingData.columns.filter(isCateg)
    .map(c => new StringIndexer()
      .setInputCol(c)
      .setOutputCol(categNewCol1(c))
      .fit(allData.select(c))
    )



    val encoder = new OneHotEncoderEstimator()
      .setInputCols(trainingData.columns.filter(isCateg).map(categNewCol1))
      .setOutputCols(trainingData.columns.filter(isCateg).map(categNewCol))
    val oneHot = encoder.fit( allData.select(trainingData.columns.filter(isCateg).map(F.col(_)):_*))

  // VectorAssembler for training features
  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")
}
