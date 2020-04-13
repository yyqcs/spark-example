package dataframe.rdd2df

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//read from origin data and store it  in MySql

//object半生对象,scala，类型滞后。，存储过程 ;文件没有表土
//D为对象名称，底层的类名为D$.
object RDD2DataFrame { //向python学习
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("RDD2DataFrame")
      .getOrCreate()

    //RDD->DataFrame:表头+内容。
    //StructField(String name, DataType dataType, boolean nullable, Metadata metadata)
    val fields = Array(StructField("name", StringType, nullable = true), StructField("age", IntegerType, nullable = true))
    val schema = StructType(fields)
    val peopleRDD = spark.sparkContext.textFile("file:///F:/code_environment/PPT/test.csv")
    val rowsRDD = peopleRDD.map(_.split(",")).map(atrtibutes => Row(atrtibutes(0), atrtibutes(1).trim.toInt))

    val peopleDF = spark.createDataFrame(rowsRDD, schema)

    //for query
    peopleDF.createOrReplaceTempView("people")
    val res = spark.sql("SELECT name,age FROM people")
    println(res.show())

  }

}

