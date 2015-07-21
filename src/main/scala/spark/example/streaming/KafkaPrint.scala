package spark.example.streaming

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
    if (args.length < 5) {
      System.err.println("Usage: KafkaPrint <zkQuorum> <group> <topics> <numThreads> <numReceivers>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val Array(zkQuorum, group, topics, numThreads, numReceivers) = args
    val sparkConf = new SparkConf().setAppName("KafkaPrint")
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint("KafkaPrint_checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStreams = (1 to numReceivers.toInt).map(_ =>
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)).toArray
    val pvDstream = ssc.union(kafkaStreams)
    pvDstream.print

    ssc.start()
    ssc.awaitTermination()
  }

}

