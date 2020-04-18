package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable

object KafkaWordConsumer {
  def main(args: Array[String]): Unit = {
    StreamLog.setStreamingLogLevels()
    val sc = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc =new StreamingContext(sc,Seconds(1))
    ssc.checkpoint("file:///E:/IdeaProjects/warehourse/checkpoints")

    val topics=Array("topic")//新版应该为数组
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:2181",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val messages=KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    //累计，增量。
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
//    val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
  }

}
