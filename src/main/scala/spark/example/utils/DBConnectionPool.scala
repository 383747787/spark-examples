package spark.example.utils

import scalikejdbc._
import scalikejdbc.config._

/**
 * Created by juanpi on 2015/7/8.
 */
object DBConnectionPool {
  def init(): Unit ={
    if(!ConnectionPool.isInitialized()){
      DBs.setup()
    }
  }
}
