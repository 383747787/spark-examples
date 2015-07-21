package spark.example.streaming

import java.util

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import org.apache.log4j._

/**
 * Created by juanpi on 2015/7/7.
 */
object KafkaPageViewProducer {
  private[this] val logger = Logger.getLogger(KafkaPageViewProducer.getClass)

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaPageViewProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <sleepSec>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, sleepSec) = args
    //val filename="hdfs://192.168.16.95:8020/user/hadoop/test/part-m-00000"
    val filename="/tmp/1w.txt"
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val file = Source.fromFile(filename)
    val datas = file.getLines().toArray[String]

    // Send some messages
    while(true){
      (1 to messagesPerSec.toInt).foreach { i =>
        val rem:Int = i % datas.length
        val message = new ProducerRecord[String, String](topic, null, datas(rem))
        producer.send(message)
      }
      logger.info(s"commit ${messagesPerSec} rows")

      Thread.sleep(sleepSec.toInt*1000)
    }

  }
}

object KafkaFileProducer {
  private[this] val logger = Logger.getLogger(KafkaPageViewProducer.getClass)

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaPageViewProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <sleepSec> <filename>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, sleepSec, filename) = args
    //val filename="hdfs://192.168.16.95:8020/user/hadoop/test/part-m-00000"
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val file = Source.fromFile(filename)
    val datas = file.getLines()

    // Send some messages
    while (datas.hasNext) {
      (1 to messagesPerSec.toInt).foreach { i =>
        val message = new ProducerRecord[String, String](topic, null, datas.next)
        producer.send(message)
      }
      logger.info(s"commit ${messagesPerSec} rows")

      Thread.sleep(sleepSec.toInt * 1000)
    }
  }
}
