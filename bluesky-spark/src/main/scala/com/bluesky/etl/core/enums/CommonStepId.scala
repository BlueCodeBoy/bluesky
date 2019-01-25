package com.bluesky.etl.core.enums

/**
  * Created by root on 18-8-17.
  */
object CommonStepId extends Enumeration {
  type CommonStepId = Value

  val TableSelector = Value(1, "TableSelector")
  val SwitchCase = Value(2, "SwitchCase")


  //正则表达式校验 （SFFieldRegular）
  val SFFieldRegular = Value(3, "SFFieldRegular")
  //数据范围校验（SFFieldInterval）
  val SFFieldInterval = Value(4, "SFFieldInterval")
  //唯一性校验（SFFieldUnique）
  val SFFieldUnique = Value(5, "SFFieldUnique")
  //空值校验（SFFieldDefect）
  val SFFieldDefect = Value(6, "SFFieldDefect")
  //字段值比较（SFFieldComparision）
  val SFFieldComparision = Value(7, "SFFieldComparision")
  //数据格式校验（FieldFormatVerify
  val FieldFormatVerify = Value(8, "FieldFormatVerify")
  //主外键一致性（ForeignToPrimary）
  val ForeignToPrimary = Value(9, "ForeignToPrimary")
  //记录数缺失（CountRecords）
  val CountRecords = Value(10, "CountRecords")
  //精度校验（DigitalPrecision）
  val DigitalPrecision = Value(11, "DigitalPrecision")
  //值域校验（SFFieldEnum）
  val SFFieldEnum =Value(12,"SFFieldEnum")
  val  KafkaConsumer = Value(13,"KafkaConsumer")
  val  KafkaProducer = Value(14,"KafkaProducer")
  val  MultiwayMergeJoin = Value(15,"MultiwayMergeJoin")
  val  TableInput = Value(16 ,"TableInput")

  def checkExists(types:String) = this.values.exists(_.toString==types) //检测是否存在此枚举值
  def showAll = this.values.foreach(println)
  def getStepById(types:String)=this.values.filter(data=>{if(data.toString.equals(types)) true else false})
}
