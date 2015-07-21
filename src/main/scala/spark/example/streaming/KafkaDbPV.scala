package spark.example.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import java.util.Date
import scalikejdbc._
import scalikejdbc.config._
import spark.example.utils.DBConnectionPool

/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaDbPV {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaDbPV <zkQuorum> <group> <topics> <numThreads> <numReceivers>")
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
    val sparkConf = new SparkConf().setAppName("KafkaDbPV")
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint("KafkaDbPV_checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStreams = (1 to numReceivers.toInt).map(_ =>
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)).toArray
    val pvDstream = ssc.union(kafkaStreams).count

    pvDstream.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd")

      rdd.foreach { record =>
        DBConnectionPool.init
        DB localTx { implicit session =>
          val pvOption = sql"select ifnull(pv,0) from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()

          pvOption match {
            case None => {
                sql"insert into temp.tmp_db_pv(period_date,pv) values(${nowDate},${record})".update.apply()
            }
            case Some(v) => {
              val pv = v + record
              sql"update temp.tmp_db_pv set pv = ${pv} where period_date=${nowDate}".update.apply()
            }
          }


        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

