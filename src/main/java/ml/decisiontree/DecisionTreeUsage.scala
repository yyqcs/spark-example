package ml.decisiontree

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}

object DecisionTreeUsage {

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
    data.createOrReplaceTempView("iris")
    val df = spark.sql("select * from iris")
    //标签列和特征列
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").
      setMaxCategories(4).fit(df)
    //预测结果重新转换为字符串label
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    //预处理与数据划分都是一样的，具体模型的时候有区别。
    //预处理与数据划分都是一样的，具体模型的时候有区别。
    //预处理与数据划分都是一样的，具体模型的时候有区别。
    val dtClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    val pipelinedClassifier = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtClassifier, labelConverter))
    val modelClassifier = pipelinedClassifier.fit(trainingData)
    val predictionsClassifier = modelClassifier.transform(testData)
    predictionsClassifier.select("predictedLabel", "label").show(20)
    val evaluatorClassifier = new MulticlassClassificationEvaluator().
                                setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluatorClassifier.evaluate(predictionsClassifier)
    println("Test Error = " + (1.0 - accuracy))
    //获取已经循环好的模型
    val treeModelClassifier = modelClassifier.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModelClassifier.toDebugString)


    //回归决策树
    val dtRegressor = new DecisionTreeRegressor().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
    val pipelineRegressor = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtRegressor, labelConverter))
    val modelRegressor = pipelineRegressor.fit(trainingData)
    val predictionsRegressor = modelRegressor.transform(testData)
    predictionsRegressor.select("predictedLabel", "label").show(20)
    val evaluatorRegressor = new RegressionEvaluator().setLabelCol("indexedLabel")
                                    .setPredictionCol("prediction").setMetricName("rmse")
    val rmse = evaluatorRegressor.evaluate(predictionsRegressor)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    val treeModelRegressor = modelRegressor.stages(2).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModelRegressor.toDebugString)



  }

}
