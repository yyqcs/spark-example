package graphx

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession

object TriangleCountUsage {
  def main(args:Array[String]):Unit={
    val spark =SparkSession
      .builder()
      .master("local[2]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc=spark.sparkContext
    val firdir="file:///D:/spark-2.4.5-bin-hadoop2.7/data/graphx/"
    val graph=GraphLoader.edgeListFile(sc,firdir+"followers.txt")
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val triCounts=graph.triangleCount().vertices
    val users=sc.textFile(firdir+"users.txt").map({line=>
      val fields=line.split(",")
      (fields(0).toLong,fields(1))
    })
    val triCountByUsername = users.join(triCounts).map {
      case (id, (username, tc)) =>
      (username, tc)
    }
    println(triCountByUsername.collect().mkString("\n"))
    spark.stop()
  }

}
