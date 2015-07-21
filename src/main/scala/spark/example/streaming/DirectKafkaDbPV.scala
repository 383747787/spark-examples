package spark.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import kafka.serializer.StringDecoder
import scalikejdbc._
import scalikejdbc.config._
import spark.example.utils.DBConnectionPool

/**
 * Created by juanpi on 2015/7/7.
 */
object DirectKafkaDbPV {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaDbPV <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()
    val appname = "DirectKafkaDbPV"

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName(appname)
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint(appname+"_checkpoint")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val pvDstream = messages.count

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
