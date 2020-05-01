package dataframe.basic

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

case class Record(key:Int,value:String)
case class Person(name: String, age: Long)//case 类相等于java的POJO类
object DataFrameBasic {
  private val firdir="file:///D:/spark-2.4.5-bin-hadoop2.7/examples/src/main/resources/"

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[*]")
      .appName("DataFrameBasic")
      .config("spark.sql.warehouse.dir", "file:///F:/code_environment/BigData/spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    val df=spark.createDataFrame((1 to 1000).map(i=>Record(i,s"val_$i")))
    df.createTempView("records")
    spark.sql("SELECT key,value FROM records WHERE key<10").collect().foreach(println)
    val count=spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*):$count")

    val rddFromSql=spark.sql("SELECT key,value FROM records WHERE key<20")
    rddFromSql.rdd.map(row=>s"Key:${row(0)},value:${row(1)}").collect().foreach(println)
    //query with LINQ-Like Scala DSL
    df.where($"key"===1).orderBy($"value".asc).select($"key").collect().foreach(println)

    df.write.mode(SaveMode.Overwrite).parquet("pair.parquest")
    val parquestFile=spark.read.parquet("pair.parquest")
    parquestFile.where($"key"===2).select($"value".as("a")).collect().foreach(println)
    //两种查询形式，1.SQL；2.scala 的DSL
    parquestFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)


    val data=spark.read.json(firdir+"people.json")
    data.show()
    data.printSchema()
    data.select("name").show()
    data.select($"name",$"age"+1).show()
    data.filter($"age">21).show()
    data.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()

    spark.newSession().sql("SELECT name FROM global_temp.people").show()
    runExample(spark)


    spark.stop()
  }
  private def runExample(spark:SparkSession):Unit={
    val df=spark.read.json(firdir+"people.json")
    df.show()
    import spark.implicits._
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()

    val peopleDS: Dataset[Person]=spark.read.json(firdir+"people.json").as[Person]
    peopleDS.show()

    val peopleDF=spark.sparkContext.textFile(firdir+"people.txt")
      .map(_.split(","))
      .map(attributes=>Person(attributes(0),attributes(1).trim.toInt))
      .toDF()
    peopleDF.createOrReplaceTempView("people")
    val teenagerDF=spark.sql("SELECT name,age FROM people WHERE age between 13 and 19 ")
    teenagerDF.map(teenager=>"name: "+teenager(0)).show()
    teenagerDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
  //第三种方式：通过StructType来创建DF.
  }
}
