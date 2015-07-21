package spark.example.streaming

import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import scalikejdbc._
import scalikejdbc.config._
import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}
import play.api.libs.json._
import spark.example.utils.DBConnectionPool

import scala.reflect.ClassTag


/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaDbUV extends Logging{

  def createContext(args: Array[String], checkpointDirectory:String): StreamingContext ={
    logInfo("creating new context")

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaDbUV")
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint(checkpointDirectory)

    var UVMapAccum = ssc.sparkContext.accumulable(Map[String,Long](),"uv info")(MapAccumulatorParam)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.map(_.split("\t")(1)).map(line => Json.parse(line)\"_id").map(_.toString())
    val pvDstream = words

    pvDstream.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd")
      DBConnectionPool.init
      val uvOption = DB readOnly { implicit session =>
        sql"select ifnull(uv,0) from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()
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
        val uvOption = sql"select ifnull(uv,0) from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()

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
    ssc
  }


  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaDbUV <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val checkpointDirectory = "KafkaDbUV_checkpoint"
    StreamingExamples.setStreamingLogLevels()
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(args,checkpointDirectory)
      })

    ssc.start()
    ssc.awaitTermination()
  }

}

object MapAccumulatorParam extends AccumulableParam[Map[String,Long],String] {
  def addAccumulator(v1:Map[String,Long],elem:String): Map[String,Long] ={
    val cnt:Long = v1.getOrElse(elem,0L)
    v1.updated(elem,cnt+1)
  }

  def zero(initialValue: Map[String,Long]): Map[String,Long] = {
    initialValue
  }
  def addInPlace(v1: Map[String,Long], v2: Map[String,Long]): Map[String,Long] = {
    v1 ++ v2.map{ case (k:String,v:Long) => k -> (v + v1.getOrElse(k,0L))}
  }
}

case class UserEvent(guid:String){
  def getGuid = guid
}

