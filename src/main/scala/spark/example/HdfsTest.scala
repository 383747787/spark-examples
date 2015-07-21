package spark.example

import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by juanpi on 2015/7/8.
 */
object HdfsTest {

  def main (args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("HdfsTest")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(args(0))
    val sqlContext = new HiveContext(sc)
    val rownum = file.count

    println(rownum)

    sc.stop()
  }
}
