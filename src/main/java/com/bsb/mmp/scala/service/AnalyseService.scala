package com.bsb.mmp.scala.service

import com.bsb.mmp.scala.entity.Analyse
import com.bsb.mmp.scala.util.MysqlConnectUtils

/**
  * @创建人 xzc
  * @创建时间 2019/10/29
  * @描述
  */
class AnalyseService {

  def add(analyse: Analyse): Boolean = {
    val conn = MysqlConnectUtils.getConnection()
    try {
      val sql = new StringBuilder()
        .append("INSERT INTO mmp_monitor_analyze_info(column_import_id, column_num,base_line,create_time)")
        .append("     VALUES(?,?,?,?)")
      val pstm = conn.prepareStatement(sql.toString())
      pstm.setObject(1, analyse.columnImportId)
      pstm.setObject(2, analyse.columnNum)
      pstm.setObject(3, analyse.baseLine)
      pstm.setObject(4, analyse.createTime)
      pstm.executeUpdate() > 0
    }
    finally {
      conn.close()
    }
  }
}
