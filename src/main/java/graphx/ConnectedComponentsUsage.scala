package graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object ConnectedComponentsUsage {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession
      .builder()
      .master("local[2]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc=spark.sparkContext

    val firdir="file:///D:/spark-2.4.5-bin-hadoop2.7/data/graphx/"
    val graph=GraphLoader.edgeListFile(sc,firdir+"followers.txt")
    val cc=graph.connectedComponents().vertices
    val users=sc.textFile(firdir+"users.txt").map({line=>
      val fields=line.split(",")
      (fields(0).toLong,fields(1))
    })
    val ccByusername=users.join(cc).map{
      case (id,(username,cc))=>(username,cc)
    }
    println(ccByusername.collect().mkString("\n"))
    spark.stop()




  }

}
