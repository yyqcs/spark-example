package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.internal.Logging

object FileStream {
  def main(args: Array[String]): Unit = {
    StreamLog.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))// 时间间隔为2秒
    //textFileStream参数应该是目录
    val lines = ssc.textFileStream("file:///D:/OS/Salver/")
    val words = lines.flatMap(_.split("\\."))
    words.cache()
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
