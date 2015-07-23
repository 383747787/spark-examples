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
import spark.example.utils.DBConnectionPool
import org.apache.hadoop.hbase.util.Bytes
import org.codehaus.jackson.map.ObjectMapper
import java.util.{Map=>JMap}
import play.api.libs.json._

/**
 * Created by juanpi on 2015/7/6.
 */
object Kafkamb_pageinfo extends Logging{

  val ACTIVE_USER = "active.user"
  val ADD_USER = "add.user"

  val NEWUSER="newuser"
  val OLDUSER="olduser"

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
    val pageinfoOri = messages.map(message => {
      parseMessage(message._2.replace("\0",""))
    }).cache()

    //过滤掉jpid和deviceid都为空的数据
    val errorPageinfo = pageinfoOri.filter(_._1.equals("deviceid:"))
    val errCnt = errorPageinfo.count
    errCnt.foreachRDD{rdd =>
      rdd.foreach { record =>
        logError("filter error mesgs:" + record)
      }
    }

    val pageinfoDstream = pageinfoOri.filter(!_._1.equals("deviceid:"))
    // 按ticks+appName分组排序，取starttime最小的记录
    // 然后从ticks_history_real表中取到初装时间和初装渠道
    val channelUserVisitDstream = pageinfoDstream.transform(firstUserChannelFunc)
                                    .transform(getUserInitialChannelFunc).cache()

    val (dayActiveUsers,hourAddUsers) = activeUser(channelUserVisitDstream)

    val dayUserDstream = dayActiveUsers.leftOuterJoin(hourAddUsers)
    saveDailyInfo(dayUserDstream)
    //channelUserVisitDstream.print
    ssc
  }

  /*
  def parseMessage(message:String):(String,MbPageInfo) = {
    val mapper = new ObjectMapper()
    val row = mapper.readValue(message.replace("\\0",""),classOf[JMap[String,String]])
    // ticks生成规则，jpid不为空的赋值jpid否则赋值"deviceid:"+deviceid
    val jpId = row.get("jpid")
    val deviceId = row.get("deviceid")
    val ticks = if(jpId.length>5){jpId}else{"deviceid:"+deviceId}
    val appName = row.get("app_name")
    (ticks,new MbPageInfo(ticks+":"+appName,row.get("starttime"),
      row.get("utm"),
      row.get("os"),
      appName,
      "",
      deviceId
    ))
  }
  */

  def parseMessage(message:String):(String,MbPageInfo) = {
    //logInfo("message:"+message)
    val row = Json.parse(message)
    // ticks生成规则，jpid不为空的赋值jpid否则赋值"deviceid:"+deviceid
    val jpId = (row\"jpid").asOpt[String].getOrElse("")
    val deviceId = (row\"deviceid").asOpt[String].getOrElse("")
    val ticks = if(jpId.length>5){jpId}else{"deviceid:"+deviceId}
    val appName = (row\"app_name").as[String]
    (ticks+":"+appName,new MbPageInfo(ticks,(row\"starttime").as[String],
      (row\"utm").as[String],
      (row\"os").as[String],
      appName,
      "",
      deviceId
    ))
  }

  /**
   * 按ticksid+appName作为key取starttime最小的一条
   */
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
      for((_,pageInfo) <- iter) yield{
        val result = ticksUtils.getTicksHistory(pageInfo.ticks,pageInfo.appName,pageInfo.utm)
        val rowkey = pageInfo.ticks + ":" + pageInfo.appName
        result match {
          case None => {
            //如果是按jpid没有找到数据再按deviceid去找
            //如果找到了将找到的initDate和utm最为初装信息
            val (ticksHistory,userType) = if(!pageInfo.ticks.startsWith("deviceid:")){
              val ticks1 = "deviceid:" + pageInfo.deviceId
              val result1 = ticksUtils.getTicksHistory(ticks1,pageInfo.appName,pageInfo.utm)
              result1 match {
                case None => (new TicksHistory(rowkey,pageInfo.starttime,pageInfo.utm),NEWUSER)
                case Some(v) => (new TicksHistory(rowkey,v.initDate,v.utm),OLDUSER)
              }
            }else {
              (new TicksHistory(rowkey,pageInfo.starttime,pageInfo.utm),NEWUSER)
            }

            ticksUtils.addTicksHistory(ticksHistory)
            val newMbPageInfo = new MbPageInfo(pageInfo.ticks,pageInfo.starttime,ticksHistory.utm,pageInfo.os,pageInfo.appName,userType,pageInfo.deviceId)
            newMbPageInfo
          }
          case Some(v) => {
            new MbPageInfo(pageInfo.ticks,pageInfo.starttime,v.utm,pageInfo.os,pageInfo.appName,OLDUSER,pageInfo.deviceId)
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
  def activeUser(mbPageInfos: DStream[MbPageInfo]) = {
    val uvKey = "activity"

    //Add timestamp which is used for resetting state as well as timeseries of dashboard
    val mbPageInfosTime = mbPageInfos.map(mbPageInfo => (new ChannelInfo(mbPageInfo.os,mbPageInfo.appName,mbPageInfo.utm),Set(mbPageInfo.ticks)))
      .transform(addTime _).cache()

    val addUserVisitTime = mbPageInfos.filter(mbPageInfo => mbPageInfo.userType==NEWUSER)
      .map(mbPageInfo => (new ChannelInfo(mbPageInfo.os,mbPageInfo.appName,mbPageInfo.utm),Set(mbPageInfo.ticks)))
      .transform(addTime _)

    //Create the stream which includes state for 24 hours
    val dayActive = mbPageInfosTime.updateStateByKey(updateTotalCountStateDay)
      .mapValues[(Long,Long)]{
      case (t: Long, ticks:Set[String]) => (t, ticks.size.toLong)
    }

    // 统计每小时的新装用户数
    val hourAdd = addUserVisitTime.updateStateByKey(updateTotalCountStateHour)
      .mapValues[(Long,Long)]{
      case (t: Long, ticks:Set[String]) => (t, ticks.size.toLong)
    }

    //Create the stream which includes state for 1 hours
    val hourActive = mbPageInfosTime.updateStateByKey(updateTotalCountStateHour)
      .mapValues[(Long,Long)]{
      case (t: Long, ticks:Set[String]) => (t, ticks.size.toLong)
    }

    (dayActive, hourAdd)
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
        if(isSameCycle(stateReceived._1, hdT,cycleType)) Some((hdT, v)) else Some((hdT, v++stateReceived._2))
    }
  }


  def isSameCycle(last:Long,now:Long,cycleType:String):Boolean = {
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

  def saveDailyInfo(dailyDstrem:DStream[(ChannelInfo,((Long,Long),Option[(Long,Long)]))]) ={
    dailyDstrem.foreachRDD { rdd =>
      val nowDate = (new DateTime()).toString("yyyy-MM-dd HH:00:00")

      rdd.foreachPartition { partitionOfRecords =>
        DBConnectionPool.init
        partitionOfRecords.foreach { record =>
          val os = record._1.os
          val appName = record._1.appName
          val utm = record._1.utm.toLong
          val activeUserCnt = record._2._1._2
          val addUserCnt = record._2._2.getOrElse((0,0))._2
          DB localTx { implicit session =>
            sql"replace into rpt.mb_users_daily_hour_real(date,os,app_name,utm_id,daily_active_users,new_users) values(${nowDate},${os},${appName},${utm},${activeUserCnt},${addUserCnt})".update.apply()
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

    //val ssc = createContext(args,checkpointDirectory)
    ssc.start()
    ssc.awaitTermination()
  }
}

case class MbPageInfo(ticks:String,starttime:String,utm:String,os:String,appName:String,userType:String,deviceId:String)

case class TicksHistory(rowkey:String,initDate:String,utm:String)

case class ChannelInfo(os:String,appName:String,utm:String)

class TicksHistoryUtils{

  var ticksHistoryTable:HTable = null
  val family = "appinfo"

  def init(): Unit ={
    val conf = HBaseConfiguration.create();
    //conf.set("hbase.zookeeper.quorum", zkQuorum);
    ticksHistoryTable = new HTable(conf,"ticks_history_real");
  }

  def init(zkQuorum:String): Unit ={
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    ticksHistoryTable = new HTable(conf,"ticks_history_real");
  }

  /**
   * 从hbase中获取初装渠道和初装时间
   *
   * @param ticks 格式
   * @param utm
   * @return
   */
  def getTicksHistory(ticks:String,appName:String,utm:String): Option[TicksHistory] ={
    val rowkey = ticks + ":" + appName
    val get =new Get(rowkey.getBytes());

    val result = ticksHistoryTable.get(get);

    if(result.isEmpty){
      None
    }else{
      // 如果ticks是以deviceid:开头的为没有jpid的数据，这个以传入的tum作为初装utm
      val initUtm = if(ticks.startsWith("deviceid:")){
        utm
      }else {
        Bytes.toString(result.getValue(family.getBytes(),"utm".getBytes()))
      }
      val ticksHistory = new TicksHistory(rowkey,
        Bytes.toString(result.getValue(family.getBytes(),"init_date".getBytes())),initUtm)
      Some(ticksHistory)
    }
  }

  def addTicksHistory(ticksHistory:TicksHistory) {
    val  put = new Put(ticksHistory.rowkey.getBytes());

    put.add(family.getBytes(),"init_date".getBytes(),ticksHistory.initDate.getBytes());
    put.add(family.getBytes(),"utm".getBytes(),ticksHistory.utm.getBytes());

    ticksHistoryTable.put(put);
  }
}
