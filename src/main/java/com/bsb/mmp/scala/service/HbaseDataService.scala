package com.bsb.mmp.scala.service

import java.text.SimpleDateFormat
import java.util.{Base64, Calendar, Date}

import com.bsb.mmp.java.util.Properity
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @创建人 xzc
  * @创建时间 2019/10/25
  * @描述
  */
class HbaseDataService {
  /**
    * scan
    */
  def readFromHBaseWithHBaseNewAPIScan(modelId: String, baseNum: Int,startTime: Date, sparkSession: SparkSession): RDD[(ImmutableBytesWritable, Result)] = {
    //屏蔽不必要的日志显示在终端上

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local[*]")
    //      .config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "mmp:mmpMonitor"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, Properity.getProperty("hbase.zookeeper.quorum"))
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Properity.getProperty("hbase.zookeeper.property.clientPort"))
    hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, Properity.getProperty("zookeeper.znode.parent"))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    var ca = Calendar.getInstance
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val dateStr = sdf.format(new Date)
    //    System.out.println(dateStr)
    val parseDate = sdf.parse(dateStr)
    ca.setTime(parseDate)
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    var startCa = Calendar.getInstance
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTime_f = sdf2.format(startTime)
    val parse_startTime = sdf.parse(startTime_f)
    startCa.setTime(parse_startTime)
    print(startTime_f)
    val startTimeFilter = new SingleColumnValueFilter(Bytes.toBytes("common"), Bytes.toBytes("create_time"), CompareOp.GREATER, Bytes.toBytes(startCa.getTime.getTime.toString))
    filterList.addFilter(startTimeFilter)
    val endTimeFilter = new SingleColumnValueFilter(Bytes.toBytes("common"), Bytes.toBytes("create_time"), CompareOp.LESS, Bytes.toBytes(ca.getTime.getTime.toString))
    filterList.addFilter(endTimeFilter)
    val valueFilter = new SingleColumnValueFilter(Bytes.toBytes("common"), Bytes.toBytes("model_id"), CompareOp.EQUAL, Bytes.toBytes(modelId))
    filterList.addFilter(valueFilter)
    val filternum = new PageFilter(baseNum) // 查询一条
    filterList.addFilter(filternum)
    scan.setFilter(filterList)

    scan.setReversed(true) // 倒序查询,查询的row位置颠倒才生效

    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hbaseRDD.foreachPartition(fp => {
      fp.foreach(f => {
        var rowkey = Bytes.toString(f._2.getRow)
        var value = Bytes.toString(f._2.getValue("varible".getBytes, "any_fea_name4".getBytes))
        var modelId = Bytes.toString(f._2.getValue("common".getBytes, "model_id".getBytes))
        var create_time = Bytes.toString(f._2.getValue("common".getBytes, "create_time".getBytes))
        var create_time_string = Bytes.toString(f._2.getValue("common".getBytes, "create_time_string".getBytes))
        println(rowkey + "--------------" + value +"----"+modelId+"------------"+create_time_string)
      })
    })

    print(hbaseRDD.count())

    if (hbaseRDD.count() < baseNum) {
      return null
    }
    return hbaseRDD
  }

}
