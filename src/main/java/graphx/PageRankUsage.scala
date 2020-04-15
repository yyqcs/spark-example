package graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PageRankUsage {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession
      .builder()
      .master("local[2]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc=spark.sparkContext
    val firdir="file:///D:/spark-2.4.5-bin-hadoop2.7/data/graphx/"
    val graph=GraphLoader.edgeListFile(sc,firdir+"followers.txt")
    val ranks=graph.pageRank(0.0001).vertices

    val users=sc.textFile(firdir+"users.txt").map(line=>{
      val fields=line.split(",")
      (fields(0).toLong,fields(1))
    })
    val rankByUsername=users.join(ranks).map({
      case (id,(username,rank))=>(username,rank)
    })

    println(rankByUsername.collect().mkString("\n"))
    spark.stop()

  }

}
