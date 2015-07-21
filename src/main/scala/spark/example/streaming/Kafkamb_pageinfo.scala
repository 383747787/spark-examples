package spark.example.streaming

import _root_.kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HTable}
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


/**
 * Created by juanpi on 2015/7/6.
 */
object Kafkamb_pageinfo extends Logging{

  def createContext(args: Array[String], checkpointDirectory:String): StreamingContext ={
    logInfo("creating new context")

    val Array(brokers, topics) = args
    val sparkConf = new SparkConf().setAppName("Kafkamb_pageinfo")
    val ssc = new StreamingContext(sparkConf, Seconds(Config.interval))
    ssc.checkpoint(checkpointDirectory)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val pageinfoDstream = messages.map(message => {
      val jsonParse = Json.parse(message._2.replace("\\0",""))
      val ticks = (jsonParse\"ticks").as[String]
      (ticks,new MbPageInfo(ticks,(jsonParse\"starttime").as[String],
        (jsonParse\"utm").as[String],
        (jsonParse\"os").as[String],
        (jsonParse\"app_name").as[String],
        ""
      ))
    })
    val channelUserVisitDstream = pageinfoDstream.transform(firstUserChannelFunc).transform(getUserInitialChannelFunc).cache()

    val activeMap:Map[String,DStream[(ChannelInfo,(Long,Long))]] = activeUser(channelUserVisitDstream)

    activeMap.get("DAU").map(dau => saveDailyInfo(dau))
    activeMap.get("DAU").map(dau => dau.print(20))
    ssc
  }

  val firstUserChannelFunc = (rdd:RDD[(String,MbPageInfo)]) => {
    rdd.reduceByKey((v1: MbPageInfo, v2: MbPageInfo) => {
      if (v1.starttime < v2.starttime) {
        v1
      } else {
        v2
      }
    })
  }

  val getUserInitialChannelFunc = (rdd:RDD[(String,MbPageInfo)]) => {
    rdd.mapPartitions((iter:Iterator[(String,MbPageInfo)]) => {
      val ticksUtils = new TicksHistoryUtils()
      ticksUtils.init()
      for((userid,pageInfo) <- iter) yield{
        val result = ticksUtils.getTicksHistory(pageInfo.ticks)
        result match {
          case None => {
            val ticksHistory = new TicksHistory(pageInfo.ticks,pageInfo.starttime,pageInfo.utm)
            ticksUtils.addTicksHistory(ticksHistory)
            val newMbPageInfo = new MbPageInfo(pageInfo.ticks,pageInfo.starttime,pageInfo.utm,pageInfo.os,pageInfo.appName,pageInfo.starttime)
            newMbPageInfo
          }
          case Some(v) => {
            new MbPageInfo(pageInfo.ticks,pageInfo.starttime,v.utm,pageInfo.os,pageInfo.appName,v.initDate)
          }
        }
      }
    }
    )
  }

  def addTime(rdd:RDD[(ChannelInfo,Set[String])]) = {
    val timestamp = System.currentTimeMillis()
    rdd.map(t => (t._1,(timestamp,t._2)))
  }

  val HLL_BITS = 24
  def activeUser(mbPageInfos: DStream[MbPageInfo]): Map[String, DStream[(ChannelInfo,(Long,Long))]] = {
    val uvKey = "activity"

    //Add timestamp which is used for resetting state as well as timeseries of dashboard
    val mbPageInfosTime = mbPageInfos.map(mbPageInfo => (new ChannelInfo(mbPageInfo.os,mbPageInfo.appName,mbPageInfo.utm),Set(mbPageInfo.ticks)))
      .transform(addTime _).cache()

    val currActive = mbPageInfosTime.mapValues[(Long,Long)]{
      case (t: Long, ticks:Set[String]) => (t, ticks.size.toLong)
    }
    //Create the stream which includes state for 24 hours
    val dayActive = mbPageInfosTime.updateStateByKey(updateTotalCountStateDay)
      .mapValues[(Long,Long)]{
      case (t: Long, ticks:Set[String]) => (t, ticks.size.toLong)
    }

    //Create the stream which includes state for 1 hours
    val hourActive = mbPageInfosTime.updateStateByKey(updateTotalCountStateHour)
      .mapValues[(Long,Long)]{
      case (t: Long, ticks:Set[String]) => (t, ticks.size.toLong)
    }

    Map("AUCURR" -> currActive, "DAU" -> dayActive, "HAU" ->hourActive)
  }

  val updateFunc = (users: Seq[String], state: Option[Set[String]]) => {
    val result = state.getOrElse(Set())
    Some(result++users)
  }

  val updateTotalCountStateDay = (values:Seq[(Long,Set[String])], state: Option[(Long,Set[String])]) => {
    updateTotalCountState(values,state,"day")
  }

  val updateTotalCountStateHour = (values:Seq[(Long,Set[String])], state: Option[(Long,Set[String])]) => {
    updateTotalCountState(values,state,"hour")
  }


  def updateTotalCountState(values:Seq[(Long,Set[String])], state: Option[(Long,Set[String])], cycleType:String):Option[(Long,Set[String])] = {
    val defaultState = (0l,Set[String]())
    values match {
      case Nil => Some(state.getOrElse(defaultState))
      case _ =>
        val hdT = values(0)._1
        val v = values.map{ case (_, a) => a}.reduce(_++_)
        val stateReceived = state.getOrElse(defaultState)
        if(checkResetState(stateReceived._1, hdT,cycleType)) Some((hdT, v)) else Some((hdT, v++stateReceived._2))
    }
  }

  def checkResetState(last:Long,now:Long,cycleType:String):Boolean = {
    val lastDt = new DateTime(last)
    val nowDt = new DateTime(now)

    if(cycleType == "day") {
      val DATE_FORMAT = "yyyy-MM-dd"
      !lastDt.toString(DATE_FORMAT).equals(nowDt.toString(DATE_FORMAT))
    }else{
      val DATEHOUR_FORMAT = "yyyy-MM-ddHH"
      !lastDt.toString(DATEHOUR_FORMAT).equals(nowDt.toString(DATEHOUR_FORMAT))
    }
  }

  def saveDailyInfo(dailyDstrem:DStream[(ChannelInfo,(Long,Long))]) ={
    dailyDstrem.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd")

      rdd.foreachPartition { partitionOfRecords =>
        DBConnectionPool.init
        partitionOfRecords.foreach { record =>
          val os = record._1.os
          val appName = record._1.appName
          val utm = record._1.utm.toLong
          val activeUsers = record._2._2
          DB localTx { implicit session =>
            sql"replace into temp.tmp_user_daily(date,os,app_name,utm_id,active_users) values(${nowDate},${os},${appName},${utm},${activeUsers})".update.apply()
          }
        }
      }
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: Kafkamb_pageinfo <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val checkpointDirectory = "Kafkamb_pageinfo_checkpoint"
    StreamingExamples.setStreamingLogLevels()
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(args,checkpointDirectory)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

case class MbPageInfo(ticks:String,starttime:String,utm:String,os:String,appName:String,guCreateTime:String)

case class TicksHistory(ticks:String,initDate:String,utm:String)

case class ChannelInfo(os:String,appName:String,utm:String)

class TicksHistoryUtils{

  var ticksHistoryTable:HTable = null
  val family = "appinfo"

  def init(): Unit ={
    val conf = HBaseConfiguration.create();
    //conf.set("hbase.zookeeper.quorum", zkQuorum);
    ticksHistoryTable = new HTable(conf,"ticks_history");
  }

  def init(zkQuorum:String): Unit ={
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    ticksHistoryTable = new HTable(conf,"ticks_history");
  }

  def getTicksHistory(ticks:String): Option[TicksHistory] ={
    val get =new Get(ticks.getBytes());

    val result = ticksHistoryTable.get(get);

    if(result.isEmpty){
      None
    }else{
      val ticksHistory = new TicksHistory(ticks,
                                          result.getValue(family.getBytes(),"init_date".getBytes()).toString,
                                          result.getValue(family.getBytes(),"utm".getBytes()).toString)
      Some(ticksHistory)
    }
  }

  def addTicksHistory(ticksHistory:TicksHistory) {
    val  put = new Put(ticksHistory.ticks.getBytes());

    put.add(family.getBytes(),"init_date".getBytes(),ticksHistory.initDate.getBytes());
    put.add(family.getBytes(),"utm".getBytes(),ticksHistory.utm.getBytes());

    ticksHistoryTable.put(put);
  }
}
