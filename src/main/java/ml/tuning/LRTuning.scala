package ml.tuning

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.network.util.TransportFrameDecoder.Interceptor
import org.apache.spark.sql.SparkSession

object LRTuning {
  case class Iris(features: Vector, label: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MySql2DataFrame")
      .config("spark.sql.warehouse.dir", "file:///E:/IdeaProjects/warehourse")
      .getOrCreate()
    import spark.implicits._
    val filepath = "file:///E:/IdeaProjects/warehourse/dataset/iris.txt"
    val data = spark.sparkContext.textFile(filepath)
      .map(_.split(","))
      .map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble), p(4))).toDF()
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val featureIndexer=new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(data)
    val labelIndexer=new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val labelConverter = new IndexToString().setInputCol("prediction").
      setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val logisticRegression = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").
      setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, logisticRegression, labelConverter))

    //不同之处！
    val paraGrid=new ParamGridBuilder()
                  .addGrid(logisticRegression.elasticNetParam,Array(0.2,0.8))
                  .addGrid(logisticRegression.regParam,Array(0.001,0.1,0.5))
                  .build()
    val cv=new CrossValidator()
      .setEstimator(lrPipeline)
      .setEvaluator(new MulticlassClassificationEvaluator()
                        .setLabelCol("indexedLabel").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paraGrid)
      .setNumFolds(3)

    val cvModel=cv.fit(trainingData)
    val lrPredictions=cvModel.transform(testData)
    lrPredictions.select("predictedLabel","label","probability")
      .collect().foreach(println)

    val evaluator=new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
    val lrAccuracy=evaluator.evaluate(lrPredictions)
    println("lrAccuracy="+lrAccuracy)

    val bestModel=cvModel.bestModel.asInstanceOf[PipelineModel]
    val lrModel=bestModel.stages(2).asInstanceOf[LogisticRegressionModel]
    lrModel.explainParam(lrModel.regParam)
    lrModel.explainParam(lrModel.elasticNetParam)
    //print best model parameters
    println("Coefficients="+lrModel.coefficientMatrix+",Interceptor="+lrModel.interceptVector+",numClasses="+
    lrModel.numClasses+",num Features="+lrModel.numFeatures)


  }




}
