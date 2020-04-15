package graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

//节点形式应该为RDD{VertexId,VD)，边为RDD[Edge[ED]]，其中VertexId为Long
object GraphxBasic {
  //例子中，节点的属性为name,position
  def main(args: Array[String]): Unit = {
    val  conf =new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
    val sc=new SparkContext(conf)
    val users:RDD[(VertexId,(String,String))]=
      sc.parallelize(Array((3L,("rxin","student")),(7L,("jgonzal","postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
      (4L,("peter", "student"))))
    val relationships:RDD[Edge[String]]=
      sc.parallelize(Array(Edge(3L,7L,"collab"),Edge(5L,3L,"advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)
    graph.triplets.map(triplet=>triplet.srcAttr._1+"is the "+triplet.attr+" of "+triplet.dstAttr._1)
      .collect().foreach(println)

  //子图的应用,删除空节点以及与之相连的边。
    val validGraph=graph.subgraph(vpred = (id,attr)=>attr._2!="Missing")
    validGraph.triplets.collect.foreach(println)
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

  }

}
