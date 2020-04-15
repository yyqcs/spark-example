package streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source


object KafkaWordProducer {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    val lines=Source.fromFile("D:/OS/Salver/vmware.log")
    while(lines.hasNext){
      val line=lines.next()
      val record=new ProducerRecord[String,String]("topic",line.toString)
      producer.send(record,new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if(metadata!=null) println("succeed")
          if(exception!=null)println("fail")
        }
      })
      producer.close()
}







  }

}
