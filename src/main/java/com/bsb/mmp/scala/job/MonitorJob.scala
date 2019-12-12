package com.bsb.mmp.scala.job

import java.util
import java.util.{Date, List, Properties}

import com.bsb.mmp.java.util.Properity
import com.bsb.mmp.scala.entity.Analyse
import com.bsb.mmp.scala.service.{AnalyseService, HbaseDataService}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.control.Breaks.{break, breakable}

/**
  * @创建人 xzc
  * @创建时间 2019/10/24
  * @描述
  */
object MonitorJob {

  val mysqlUrl = Properity.getProperty("spring.datasource.druid.url")
  val mysqlUsername = Properity.getProperty("spring.datasource.druid.username")
  val mysqlPassword = Properity.getProperty("spring.datasource.druid.password")
  val readConnProp: Properties = new Properties
  readConnProp.put("driver", "com.mysql.jdbc.Driver")
  readConnProp.put("user", mysqlUsername)
  readConnProp.put("password", mysqlPassword)
  readConnProp.put("fetchsize", "200")
  val spark = SparkSession
    .builder()
    .appName("mmp")
    .config("spark.network.timeout", 300)
    .config("spark.worker.timeout", 500)
    .config("spark.core.connection.ack.wait.timeout", 600)
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.debug.maxToStringFields", 1000)
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    analyse()
  }

  def analyse(): Unit = {
    val mmpModelInfoSql =
      """
        |(select REQUEST_ID,monitor,monitor_swatch from mmp_model_info where monitor='2') t1
      """.stripMargin
    val mmpModelInfo: Dataset[Row] = spark.read.jdbc(mysqlUrl, mmpModelInfoSql, readConnProp)
    val mmpModelInfoList: List[Row] = mmpModelInfo.collectAsList
    var i: Int = 0
    for (i <- 0 to mmpModelInfoList.size() - 1) {
      val monitorInfo: Dataset[Row] = spark.read.jdbc(mysqlUrl
        , "(SELECT b.id,b.model_id,b.varible_name,b.input_output,b.column_is_monitor,b.psi_is_monitor from mmp_model a " +
          "LEFT JOIN mmp_monitor_info b on a.REQUEST_ID=b.model_id where b.is_delete='3' and a.PARENT_ID='" + mmpModelInfoList.get(i).get(0).toString + "') t2"
        , readConnProp)
      val monitorInfoList: List[Row] = monitorInfo.collectAsList
      var j: Int = 0
      var baseNum: Int = Integer.parseInt(mmpModelInfoList.get(i).get(2).toString)
      if(monitorInfoList != null && monitorInfoList.size()>0){
        var modelId = monitorInfoList.get(0).get(1).toString

          for (j <- 0 to monitorInfoList.size() - 1) {
            val monitorUse: Dataset[Row] = spark.read.jdbc(mysqlUrl
              , "(SELECT use_time from mmp_monitor_use a WHERE monitor_id='" + monitorInfoList.get(j).get(0).toString + "') t2"
              , readConnProp)
            val monitorUseList: List[Row] = monitorUse.collectAsList

            var hbaseRDD = new HbaseDataService().readFromHBaseWithHBaseNewAPIScan(modelId, baseNum,monitorUseList.get(0).getTimestamp(0),spark)
            breakable({
              if (hbaseRDD == null) {
                break
              }
            //mmp_column_import查询分栏信息
            val columnImport: Dataset[Row] = spark.read.jdbc(mysqlUrl
              , "(SELECT a.order_num,a.column_value,a.input_output,a.varible_name,a.id from mmp_column_import a WHERE a.is_delete!='1' and a.monitor_id='" + monitorInfoList.get(j).get(0).toString + "') t2"
              , readConnProp)
            val columnImportList: List[Row] = columnImport.collectAsList
            var k: Int = 0
            for (k <- 0 to columnImportList.size() - 1) {
              var columnValue = columnImportList.get(k).get(1).toString
              var columenName = columnImportList.get(k).get(3).toString
              var columnImportId = columnImportList.get(k).get(4).toString
              var countNum: Int = 0
              if (columnValue.contains("(") || columnValue.contains("[") || columnValue.contains(")") || columnValue.contains("]")) {
                var columnValueSub = columnValue.substring(1, columnValue.length - 1)
                var arr = columnValueSub.split(",", 0)
                //循环查询结果计算数据
                hbaseRDD.foreachPartition(fp => {
                  fp.foreach(f => {
                    val rowkey = Bytes.toString(f._2.getRow)
                    val variableType = columnImportList.get(k).get(2).toString
                    var value = ""
                    if ("1".equals(variableType)) {
                      value = Bytes.toString(f._2.getValue("varible".getBytes, columnImportList.get(k).get(3).toString.getBytes))
                    } else {
                      value = Bytes.toString(f._2.getValue("result".getBytes, columnImportList.get(k).get(3).toString.getBytes))
                    }
                    if(value!=null && value!=""){
                      if (columnValue.contains("[") && columnValue.contains("]")) {
                        if (value.toDouble >= arr(0).toDouble && value.toDouble <= arr(1).toDouble) {
                          countNum += 1
                        }
                      } else if (columnValue.contains("[")) {
                        if ("inf".equals(arr(1).toLowerCase.toString)) {
                          if (value.toDouble >= arr(0).toDouble) {
                            countNum += 1
                          }
                        } else if (value.toDouble >= arr(0).toDouble && value.toDouble < arr(1).toDouble) {
                          countNum += 1
                        }
                      } else if (columnValue.contains("]")) {
                        if ("-inf".equals(arr(0).toLowerCase.toString)) {
                          if (value.toDouble <= arr(1).toDouble) {
                            countNum += 1
                          }
                        } else if (value.toDouble > arr(0).toDouble && value.toDouble <= arr(1).toDouble) {
                          countNum += 1
                        }
                      } else {
                        if ("-inf".equals(arr(0).toLowerCase.toString)) {
                          if (value.toDouble < arr(1).toDouble) {
                            countNum += 1
                          }
                        } else if ("inf".equals(arr(1).toLowerCase.toString)) {
                          if (value.toDouble > arr(0).toDouble) {
                            countNum += 1
                          }
                        } else if (value.toDouble > arr(0).toDouble && value.toDouble < arr(1).toDouble) {
                          countNum += 1
                        }
                    }
                    }
                  })
                  val Analyse = new Analyse(columnImportId, countNum, baseNum, new Date())
                  new AnalyseService().add(Analyse)
                })
              } else {
                //循环hbase数据进行匹配
                hbaseRDD.foreachPartition(fp => {
                  fp.foreach(f => {
                    val variableType = columnImportList.get(k).get(2).toString
                    var value = ""
                    if ("1".equals(variableType)) {
                      //入参
                      value = Bytes.toString(f._2.getValue("varible".getBytes, columnImportList.get(k).get(3).toString.getBytes))
                    } else {
                      //出参
                      value = Bytes.toString(f._2.getValue("result".getBytes, columnImportList.get(k).get(3).toString.getBytes))
                    }
                    if(value!=null && value!=""){
                      if (value.toDouble == columnValue.toDouble) {
                        countNum = countNum + 1
                      }
                    }
                  })
                  val Analyse = new Analyse(columnImportId, countNum, baseNum, new Date())
                  new AnalyseService().add(Analyse)
                })
              }
            }
            })
          }

      }
    }
  }
}
