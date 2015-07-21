package spark.example.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaPV {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaPV <zkQuorum> <group> <topics> <numThreads>")
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

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaPV")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("KafkaPV_checkpoint")

    val initialRDD = ssc.sparkContext.parallelize(List(("pv", 0)))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val pvDstream = lines.map(x => ("pv", 1))
    val stateDstream = pvDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner (ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
