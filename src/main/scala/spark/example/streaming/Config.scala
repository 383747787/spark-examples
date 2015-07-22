package spark.example.streaming

import scala.concurrent.duration.Duration

/**
 * Created by juanpi on 2015/7/9.
 */
object Config {

  val interval = System.getProperty("spark.mystreaming.batch.interval", "60").toInt

  val hbaseZkQuorum = ""
}
