package spark.example.streaming

import KafkaDbUV._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import play.api.libs.json.Json
import scalikejdbc._
import scalikejdbc.config._
import spark.example.utils.DBConnectionPool

/**
 * Created by juanpi on 2015/7/7.
 */
object KafkaSqlUV extends Logging{
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaSqlUV <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val appname = "KafkaSqlUV"

    StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName(appname)
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint(appname + "_checkpoint")
    var UVMapAccum = ssc.sparkContext.accumulable(Map[String,Long](),"uv info")(MapAccumulatorParam)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2).map(_.split("\t")(1))

    lines.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd")
      DBConnectionPool.init
      val uvOption = DB readOnly { implicit session =>
        sql"select uv from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()
      }
      uvOption match {
        case None =>
          UVMapAccum.value = Map()
        case Some(v) =>
      }

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val pcEventDataFrame = sqlContext.read.json(rdd)
      logInfo(pcEventDataFrame.schema.treeString)

      pcEventDataFrame.registerTempTable("pc_event")

      val wordCountsDataFrame = sqlContext.sql("select `_id`,count(*) from pc_event group by `_id`")
      var period_uv = UVMapAccum.value.size.toLong

      wordCountsDataFrame.map(_.getString(0)).foreach { record =>
        UVMapAccum += record
      }
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
      logInfo("-----------------------------------------"+UVMapAccum.value.size.toString)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
