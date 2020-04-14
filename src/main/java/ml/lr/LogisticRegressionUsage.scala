package ml.lr

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}


object LogisticRegressionUsage {

  case class Iris(features: Vector, label: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MySql2DataFrame")
      .config("spark.sql.warehouse.dir", "file:///E:/IdeaProjects/warehourse")
      .getOrCreate()
    //此spark是指SparkSession.builder类的实例，需导入，否则，data会报错
    import spark.implicits._
    val filepath = "file:///E:/IdeaProjects/warehourse/dataset/iris.txt"
    val data = spark.sparkContext.textFile(filepath)
      .map(_.split(","))
      .map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble), p(4))).toDF()
//    data.show()
    data.createOrReplaceTempView("iris")
    //choose two class
    val df = spark.sql("select * from iris where label != 'Iris-setosa'")
    //获取 index与features
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(df)
    //划分数据集：训练集和测试集
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    //循环次数为10次，正则化项为0.3等
    val logisticRegression = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").
      setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

    //for display result
    val labelConverter = new IndexToString().setInputCol("prediction").
      setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, logisticRegression, labelConverter))
    //fit,PipelineModel可以通过调用.stage方法获取某一个阶段的结果
    val lrPipelineModel = lrPipeline.fit(trainingData)
    //prediction
    val lrPredictions = lrPipelineModel.transform(testData)

//    lrPredictions.select("predictedLabel", "label",  "probability").collect().foreach(println)

    //evaluation
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    val lrAccuracy = evaluator.evaluate(lrPredictions)
    println("Test Error = " + (1.0 - lrAccuracy)*100+"%")

  }


}
