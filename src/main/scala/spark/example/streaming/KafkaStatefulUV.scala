package spark.example.streaming

import _root_.kafka.serializer.StringDecoder
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
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spark.example.utils.DBConnectionPool

import scala.reflect.ClassTag


/**
 * Created by juanpi on 2015/7/6.
 */
object KafkaStatefulUV extends Logging{

  def createContext(args: Array[String], checkpointDirectory:String): StreamingContext ={
    logInfo("creating new context")

    implicit val formats = DefaultFormats

    val updateFunc = (users: Seq[String], state: Option[Set[String]]) => {

      val result = state.getOrElse(Set())

      Some(result++users)
    }

    val newUpdateFunc = (iterator: Iterator[(String, Seq[String], Option[Set[String]])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val appName = "KafkaStatefulUV"
    val Array(brokers, topics) = args
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint(checkpointDirectory)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)
    val pageInfoDstream = messages.map(line => {
      val result = parse(line.trim)
      ((result \ "_id").extract[String],new PageInfo((result\"_id").extract[String],(result\"utm").extract[String],(result\"timestamp").extract[String]))
    })

    val userChannelDstream = pageInfoDstream.transform[(String,String)](getUserChannel)

    val uvDstream = userChannelDstream.updateStateByKey[Set[String]](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    uvDstream.print
    /*
    count.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd")
      DBConnectionPool.init
      val uvOption = DB readOnly { implicit session =>
        sql"select ifnull(uv,0) from temp.tmp_db_pv where period_date=${nowDate}".map(_.long(1)).single.apply()
      }
      */

      /*
      uvOption match {
        case None =>
          uvDstream.filter()
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
      */
    //}
    ssc
  }

  val getUserChannel = (rdd:RDD[(String,PageInfo)]) => {
    rdd.reduceByKey((v1:PageInfo,v2:PageInfo) =>{
      if(v1.visitTimestamp<v2.visitTimestamp){
        v1
      }else{
        v2
      }
    })
      .mapPartitions((iter:Iterator[(String,PageInfo)]) => {
      DBConnectionPool.init
      for((userid,pageInfo) <- iter) yield{
        val channel = getUserChannelInfo(pageInfo.userId)
        channel match {
          case None => {
            addUserChannelInfo(pageInfo.userId, pageInfo.channel)
            (pageInfo.channel,pageInfo.userId)
          }
          case Some(v) => {
            (v,pageInfo.userId)
          }
        }
      }
    }
    ).distinct()
  }

  def getUserChannelInfo(userId:String): Option[String] ={
    val channelOption = DB readOnly { implicit session =>
      sql"select utm_id from temp.user_channel_info where guid=${userId}".map(_.string("utm_id")).single.apply()
    }
    channelOption
  }

  def addUserChannelInfo(userId:String,channel:String) ={
    DB localTx { implicit session =>
      sql"insert into temp.user_channel_info(guid,utm_id) values(${userId},${channel})".update.apply()
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: KafkaStatefulUV <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val checkpointDirectory = "KafkaStatefulUV_checkpoint"
    StreamingExamples.setStreamingLogLevels()
    /*
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(args,checkpointDirectory)
      })
    */
    val ssc = createContext(args,checkpointDirectory)
    ssc.start()
    ssc.awaitTermination()
  }

  def getFlatMapList(uvKey:String,x:UserEvent) = Nil

  def addTime[U](rdd:RDD[(Long, U)]):RDD[(Long,(Long,U))] = rdd.map{
    (100L,_)
  }
/*
  val HLL_BITS = 24
  def uniqueVisitors(batchLength: Int, filteredEvents: DStream[UserEvent]): Map[String, DStream[Long]] = {
    val uvKey = "UV"
    //Create both make*model and make base aggregations
    val makeModelCount = filteredEvents.flatMap((x: UserEvent) => {
      //CAN use same as only counts distinct
      val hll = new HyperLogLogMonoid(HLL_BITS)
      getFlatMapList(uvKey, x).zip(List(hll(x.getGuid.hashCode),hll(x.getGuid.hashCode)))
    }).persist()
    //Initial microbatch aggregation
    val makeModelUniques = makeModelCount.reduceByKey((hll1, hll2) => hll1 + hll2)
    //Add timestamp which is used for resetting state as well as timeseries of dashboard
    val makeModelUniquesTime = makeModelUniques.transform(rdd => {
      rdd.map((100L,_))
    })

    //Create the stream which has microbatch uniques
    val curUniques = makeModelUniquesTime.mapValues{
      case (t: Long, hll:HLL) => (t, hll.estimatedSize.toLong)
    }
    //Create the stream which includes state for 24 hours
    val stateUniques = makeModelUniquesTime.updateStateByKey(updateTotalCountState[HLL])
      .mapValues{
      case (t: Long, hll:HLL) => (t, hll.estimatedSize.toLong)
    }

    Map("UVCURR" -> curUniques, "UVTOTAL" -> stateUniques)
  }

  def updateTotalCountState[U](values: Seq[(Long, U)], state: Option[(Long, U)])(implicit monoid: Monoid[U], ct: ClassTag[U]): Option[(Long, U)] = {
    val defaultState = (0l, monoid.zero)
    values match {
      case Nil => Some(state.getOrElse(defaultState))
      case _ =>
        val hdT = values(0)._1
        // The reduction logic is now contained in the monoid definitions as opposed to thest functions. We can instead distil this to what is takes to update state
        val v = values.map{case (_, a) => a}.reduce(monoid.plus)
        val stateReceived = state.getOrElse(defaultState)
        if(checkResetState(stateReceived._1, hdT)) Some((hdT, v)) else Some((hdT, monoid.plus(v, stateReceived._2)))
    }
  }
*/
  // def checkResetState(last:Long,now:Long): Boolean = false

}

case class PageInfo(userId:String,channel:String,visitTimestamp:String)

