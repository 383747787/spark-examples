package spark.example

/**
 * Created by juanpi on 2015/7/9.
 */
object Config {

  val interval = System.getProperty("streaming.batch.interval", "60").toInt
}
