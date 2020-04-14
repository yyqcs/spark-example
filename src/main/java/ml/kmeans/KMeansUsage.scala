package ml.kmeans

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

//kMeans主要对特征(features,全是float，列)进行聚类，没有label
object KMeansUsage {
  case class Iris(features: Vector)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MySql2DataFrame")
      .config("spark.sql.warehouse.dir", "file:///E:/IdeaProjects/warehourse")
      .getOrCreate()
    import spark.implicits._
    val filepath = "file:///E:/IdeaProjects/warehourse/dataset/iris.txt"
    val df = spark.sparkContext.textFile(filepath)
      .map(line =>Iris(Vectors.dense(line.split(",").filter(p => p.matches("\\d*(\\.?)\\d*"))
        .map(_.toDouble)) )).toDF()
    val kmeansModel = new KMeans().setK(3).setFeaturesCol("features").setPredictionCol("prediction").fit(df)
    val results = kmeansModel.transform(df)

    results.collect().foreach(row =>println( row(0) + " is predicted as cluster " + row(1)))


    println("kmeansModel.summary()="+kmeansModel.summary.trainingCost)
  }



}
