package spark.example

import org.apache.spark._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by juanpi on 2015/7/8.
 */
object PopHourUV {

  def main (args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PopHourUV datetime")
      System.exit(1)
    }

    val runDatetimeStr = args(0)
    val sparkConf = new SparkConf().setAppName("PopHourUV-"+runDatetimeStr)
    val sc = new SparkContext(sparkConf)

    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    //时间解析
    //获取传入日期小时内的文件列表
    val runDatetime = DateTime.parse(runDatetimeStr, format);
    val startDatetime = new DateTime(runDatetime.getYear,runDatetime.getMonthOfYear,runDatetime.getDayOfMonth,
                                      runDatetime.getHourOfDay,0,0)
    val endDatetime = startDatetime.plusHours(1)
    val intervalMillis = Config.interval*1000

    val filePrefix="/tmp/spark/mb_pageinfo"
    val fileList:Seq[String] = startDatetime.getMillis.until(endDatetime.getMillis,intervalMillis).map(milliseconds =>
      filePrefix + "-" + (milliseconds+intervalMillis)
    )

    val file = sc.textFile(fileList.mkString(","))
    val rownum = file.count

    println(rownum)

    sc.stop()
  }
}
