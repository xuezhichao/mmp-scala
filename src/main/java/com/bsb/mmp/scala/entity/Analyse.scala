package com.bsb.mmp.scala.entity

import java.util.Date


/**
  * @创建人 xzc
  * @创建时间 2019/10/30
  * @描述
  */
class Analyse {
  var columnImportId:String = ""
  var columnNum:Int = 0
  var baseLine:Int = 0
  var createTime:Date = null

  def this(columnImportId:String,columnNum:Int,baseLine:Int,createTime:Date){
    this()
    this.columnImportId = columnImportId
    this.columnNum = columnNum
    this.baseLine = baseLine
    this.createTime = createTime
  }
}
