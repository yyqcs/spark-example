package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketSoruceUsage {
  def main(args: Array[String]): Unit = {
    StreamLog.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val ip="localhost"
    val port=7777
    val lines = ssc.socketTextStream(ip,port, StorageLevel.MEMORY_AND_DISK_SER)
    val words=lines.flatMap(_.split(" "))
    val wordCounts=words.map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
