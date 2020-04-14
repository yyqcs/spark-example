package rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
object RDDCommonUsage {
  def main(args: Array[String]): Unit = {
    //这是spark安装目录中自带的测试数据。
    val filepath="file:///D:/spark-2.4.5-bin-hadoop2.7/examples/src/main/resources/people.csv"
    val  conf =new SparkConf().setMaster("local[2]").setAppName("common usage")
    val sc=new SparkContext(conf)
    val rdd=sc.textFile(filepath)
    val  rdd1=sc.parallelize(Array(1,2,3,4,5))

    //转换操作
    rdd.map(line=>(line,1))
    rdd.filter(line=>line.contains("xiaohua"))
    rdd.flatMap(line=>line.split(","))
    var x=rdd1.map(x=>(x,1))
    x.reduceByKey((a,b)=>a+b)//有K,V=>K,V1;操作是针对value而言的，groupByKey也是针对value的
    rdd.flatMap(_.split(",")).map(word=>(word,1)).groupByKey().foreach(println)//K,V=>K,Iterable
    //groupByKey只能生产一个序列，本身不能自定义函数，需要先用groupByKey生成RDD，然后再map。
    //而reduceByKey能够在本地先进行merge操作，并且merge操作可以通过函数自定义。
    x.sortByKey()
    x.keys
    x.values
    x.join(x).foreach(println)//join键值对的内连接操作，key相等时，才连接。(K,V1)和(K,V2)---(K,(V1,V2))


    //行动操作
    rdd1.count()
    rdd1.collect()
    rdd1.first()
    rdd1.take(2)
    rdd1.reduce((a,b)=>a+b)
    rdd1.foreach(println)

    //实例

    //1.wordcount
    val wordCount1=wordCount(rdd)
    wordCount1.cache()//中间结果暂存到内存中。
    wordCount1.collect().foreach(println)

    //2.图书的平均销量，数据格式为(key,value),key表示图书名称，value表示某天图书销量.


  }
  def wordCount(rdd:RDD[String]) :RDD[(String,Int)]={
    val wordCount :RDD[(String,Int)]= rdd.flatMap(line=>line.split(","))
      .map(word=>(word,1)).reduceByKey((a,b)=>a+b)

    wordCount
  }


}
