package ml_spark.tranform

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

object TfIdf extends App{
  val spark = utils.util.initSpark("tf-idf")
  val sentencesData = spark.createDataFrame(Seq(
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
  )).toDF("label", "sentence")

  val tokenizer = new Tokenizer()
    .setInputCol("sentence")
    .setOutputCol("words")
  val wordData = tokenizer.transform(sentencesData)
  wordData.show(false)
  val hashingTF = new HashingTF()
    .setInputCol("words")
    .setOutputCol("rawFeatures")
    //.setNumFeatures(28)
  val featurizedData = hashingTF.transform(wordData)
  //features:dimensions,[index],[tf]
  featurizedData.show(false)
  val idf = new IDF()
    .setInputCol("rawFeatures")
    .setOutputCol("features")
  val idfModel = idf.fit(featurizedData)
  val rescaledData = idfModel.transform(featurizedData)
  //features:dimensions,[index],[tf-idf]
  rescaledData.select("label", "features")
    .show(false)

}
