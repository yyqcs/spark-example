package dataframe.mysql2df

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Mysql2DataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MySql2DataFrame")
      .config("spark.sql.warehouse.dir", "file:///F:/code_environment/BigData/spark-warehouse")
      .getOrCreate()
    val url="jdbc:mysql://localhost:3306/spark?serverTimezone=UTC"
    val jdbcDF = spark.read.format("jdbc")
      //spark is dbname;?serverTimezone is necessary for MySql8
      .option("url",url )
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1995")
      .option("dbtable", "student")
      .load()
    //insert new data into MySQL ,table fields :(| id|name|gender|age|)

    //header
    val schema = StructType(List(StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true)))
    //contents
    val studRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))
    val rowRDD = studRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    val studDF = spark.createDataFrame(rowRDD, schema)
    //prepare for write
    val prop = new Properties()
    prop.put("driver","com.mysql.cj.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "1995")
    studDF.write.mode("append").jdbc(url, "spark.student", prop)


    println(jdbcDF.show())
  }

}
