package com.bsb.mmp.scala.util

import java.sql.{Connection, DriverManager}

import com.bsb.mmp.java.util.Properity

/**
  * @创建人 xzc
  * @创建时间 2019/10/24
  * @描述
  */
object MysqlConnectUtils {

  val url = Properity.getProperty("spring.datasource.druid.url")
  val username = Properity.getProperty("spring.datasource.druid.username")
  val password = Properity.getProperty("spring.datasource.druid.password")
  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed() || conn != null) {
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }
}
