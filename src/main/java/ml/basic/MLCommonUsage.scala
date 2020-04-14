package ml.basic

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

//ML for dataframe; MLlib for rdd
/*
Estimator需要指定输入列、输出列。
 */
object MLCommonUsage{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MySql2DataFrame")
      .config("spark.sql.warehouse.dir", "file:///E:/IdeaProjects/warehourse")
      .getOrCreate()

//    tfIDF(spark)
//    wordToVec(spark)
//    countVector(spark)
//      stringIndex(spark)
    chiFeatureSelect(spark)
  }

  def readTrainData():Seq[(Long,String,Double)]={
    Seq((0L, "a b c d e spark", 1.0), (1L, "b d", 0.0),(2L, "spark f g h", 1.0),(3L, "hadoop mapreduce", 0.0))
  }
  def readTestData():Seq[(Long,String)]={
    Seq((4L, "spark i j k"),(5L, "l m n"),(6L, "spark a"),(7L, "apache hadoop"))
  }
  def tfIDF(spark:SparkSession) :Unit={
    val sentenceData = spark.createDataFrame(Seq((0, "I heard about Spark and I love Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat"))).toDF("label","sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")//分词，追加列words
    val tf= new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val pipeline = new Pipeline().setStages(Array(tokenizer, tf, idf))
    val model=pipeline.fit(sentenceData)
    model.transform(sentenceData).select("features", "label").take(3).foreach(println)
  }
  def lr(spark:SparkSession):Unit={
    val training =spark.createDataFrame(readTrainData()).toDF("id", "text", "label")

    //each stage of pipeline
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    //using array to assemble stage into pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    //fit model
    val model = pipeline.fit(training)

    //test
    val testData=spark.createDataFrame(readTestData()).toDF("id","text")
    model.transform(testData).select("id","text","probability","prediction").collect().foreach(println)
  }
  def wordToVec(spark:SparkSession):Unit={
    //array---Tuple
    val documentDF = spark.createDataFrame(Seq("Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")).map(Tuple1.apply)).toDF("text")
    //设置特征向量维度为3.
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)
    val model=word2Vec.fit(documentDF)
    val result=model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }
  def countVector(spark:SparkSession):Unit={
    val df = spark.createDataFrame(Seq((0, Array("a", "b", "c")),(1, Array("a", "b", "b", "c", "a")))).toDF("id", "words")
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2).fit(df)
    cvModel.transform(df).show(false)
  }

  def stringIndex(spark:SparkSession):Unit={
    val df1 = spark.createDataFrame(Seq((0, "a"),(1, "b"),(2, "c"),(3, "a"), (4, "a"), (5, "c"))).toDF("id", "category")
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
    val model = indexer.fit(df1)
    model.transform(df1).show()

    //转换为label
    val indexed = model.transform(df1)
    val converter = new IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory")
    val converted = converter.transform(indexed)
    converted.select("id", "originalCategory").show()

  }
  def chiFeatureSelect(spark:SparkSession):Unit={

    val df = spark.createDataFrame(Seq(
       (1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1),
       (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0),
       (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0)
       )).toDF("id", "features", "label")
    val selector = new ChiSqSelector().setNumTopFeatures(1)
                                      .setFeaturesCol("features").setLabelCol("label")
                                      .setOutputCol("selected-feature")
    val model = selector.fit(df)
    model.transform(df).show(false)

  }
}
