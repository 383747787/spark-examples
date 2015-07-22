package spark.example.streaming

import _root_.kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import scalikejdbc._
import spark.example.utils.DBConnectionPool

/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaPrint {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: KafkaPrint <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val sparkConf = new SparkConf().setAppName("KafkaPrint")
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))


    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.print

    ssc.start()
    ssc.awaitTermination()
  }

}

