package spark.example.streaming

import org.apache.spark.{Logging, _}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import scalikejdbc._
import scalikejdbc.config._

/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaTestDbUV extends Logging{
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaDbUV <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    DBs.setupAll()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaDbUV")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("KafkaDbUV_checkpoint")
    var UVMapAccum = ssc.sparkContext.accumulable(Map[String,Long](),"uv info")(MapAccumulatorParam)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.map(_.split(" ").head)
    val pvDstream = words

    pvDstream.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd")
      val uvOption = DB readOnly { implicit session =>
        sql"select uv from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()
      }
      uvOption match {
        case None =>
          UVMapAccum.value = Map()
        case Some(v) =>
      }

      rdd.foreach { record =>
        UVMapAccum += record
      }
      var period_uv = UVMapAccum.value.size.toLong
      DB localTx { implicit session =>
        val uvOption = sql"select uv from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()

        uvOption match {
          case None => {
            sql"insert into temp.tmp_db_pv(period_date,uv) values(${nowDate},${period_uv})".update.apply()
          }
          case Some(v) => {
            sql"update temp.tmp_db_pv set uv = ${period_uv} where period_date=${nowDate}".update.apply()
          }
        }
      }
      logInfo(UVMapAccum.value.toString)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}



