package spark.example.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scalikejdbc._
import scalikejdbc.config._
import org.joda.time.DateTime
import spark.example.utils.DBConnectionPool

/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaPeriodPV {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaPeriodPV <zkQuorum> <group> <topics> <numThreads>")
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
    val sparkConf = new SparkConf().setAppName("KafkaPeriodPV")
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint("KafkaPeriodPV_checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val pvDstream = lines.count

    pvDstream.foreachRDD { rdd =>
      rdd.foreach { record =>
        DBConnectionPool.init
        val now = new DateTime()
        val count = DB autoCommit { implicit session =>
          sql"insert into temp.tmp_p7eriod_pv(period_datetime,pv) values(${now},${record})".update.apply()
        }
        print(count)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

