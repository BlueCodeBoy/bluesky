package com.bluesky.etl.core.thread

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bluesky.etl.core.enums.ConstanceType
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by root on 18-11-14.
  */
case object StatisticRowObject extends Logging{
  val obj:Object = new Object()
  val s1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val st = new  SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  /**
    * check database
    * @param dataFrame
    */
  def checkDataBase(dataFrame: (Long,Long,DataFrame),sparkSession: SparkSession):Unit={
    val hasReportDatabase = sparkSession.sql(s"show databases  like '${ConstanceType.HIVE_STREAMING_DATABASE_NAME.toString}'").count()
    if (hasReportDatabase == 0) {
      sparkSession.sql(s"create database  ${ConstanceType.HIVE_STREAMING_DATABASE_NAME.toString}").count()
    }
    sparkSession.sql(s"use ${ConstanceType.HIVE_STREAMING_DATABASE_NAME}").count()

  }

  /**
    *
    * @param dataFrame
    */
  def saveDataToHive(dataFrame: (Long,Long,DataFrame),fist:Boolean,sparkSession: SparkSession,
   transName:String
                    ):Unit={
    obj.synchronized{
      var pk:Array[String] = null
      var op_ts:String = null
      var current_ts:String = null
      var timestamp:Timestamp = null
      var tmpAr = new ArrayBuffer[Any]()
      val fields = dataFrame._3.schema.fieldNames
      var costTime:Long =0
      val count = dataFrame._3.count()
      var date:Date = null
      val saveData = dataFrame._3.rdd.map(row=>{
        var ar = new ArrayBuffer[Any](8)
        //count
        ar+=count


        //current_ts
        if(row.fieldIndex("current_ts")!= -1){
          current_ts = row.getString(row.fieldIndex("current_ts"))
          if(null!=current_ts){
            try{
              if (current_ts.length > 23) {
                current_ts = current_ts.substring(0, 23)
              }
              ar += st.parse(current_ts).getTime
            }catch {
              case e:Exception=>{
                ar+=0L
              }
            }
          }else{
            ar+=0L
          }
        }else{
          ar+=0L
        }
        //op_ts
        if(row.fieldIndex("op_ts")!= -1){
          op_ts = row.getString(row.fieldIndex("op_ts"))
          if(null!=op_ts){
            try{
              if (op_ts.length > 23) {
                op_ts = op_ts.substring(0, 23)
              }
              date = s1.parse(op_ts)
              if((System.currentTimeMillis()-date.getTime)>28800000) {
                val calendar = Calendar.getInstance
                calendar.setTime(date)
                calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8)
                ar += calendar.getTime.getTime
              }else{
                ar += date.getTime
              }
            }catch {
              case e:Exception=>{
                ar+=0L
              }
            }
          }else{
            ar+=0L
          }
        }else{
          ar+=0L
        }
         //start
        ar+=dataFrame._1
        ar+=dataFrame._2
//        bluesky_timestamp
//        if(row.fieldIndex("bluesky_timestamp") != -1){
//          timestamp = row.getAs[Timestamp]("bluesky_timestamp")
//          bluesky_start
//          ar+=timestamp.getTime
//          end
//          ar+=endTime
//        }else{
//          ar+=null
//          ar+=endTime
//        }
        //data
        if(fields.indexOf("primary_keys") != -1 && row.fieldIndex("primary_keys")!= -1 ){
          pk = row.getAs("primary_keys").asInstanceOf[String].replace("[","").replace("]","").replaceAll("\"","").split(",")
          if(pk!=null && pk.length!=0){
            tmpAr =  new ArrayBuffer[Any]()
            for(field<-fields){
              for(key<-pk) {
                if (field.indexOf(key) != -1) {
                  if (row.fieldIndex(field) != -1) {
                    if(tmpAr.length<pk.length){
                      tmpAr += row.get(row.fieldIndex(field))
                    }
                  }
                }
              }
            }
            ar+=tmpAr.mkString(",")
          }else{
            ar+=null
          }
        }else{
          ar+=null
        }
        if(null!=op_ts &&0!=op_ts){
          if(null == ar(2)){
            ar+= -1
          }else{
            if(count>0){
//              costTime = dataFrame._1+ ((dataFrame._2-dataFrame._1)/count) - ar(2).asInstanceOf[Long]
              costTime = dataFrame._2 - ar(2).asInstanceOf[Long]
              //5*60*1000
              if(costTime < 30000){
                if(costTime<0){
                  costTime = -2
                }
                ar+=costTime
              }else{
                ar+= 0-costTime
              }
            }else{
              ar+=null
            }
          }
        }else{
          ar += -4
        }
        //tranc name
        ar+=transName
        Row.fromSeq(ar)
      })
      val fdataFrame = sparkSession.createDataFrame(saveData,getSchema())
      if(!fdataFrame.rdd.isEmpty()){
        try{
          sparkSession.sql(s"use ${ConstanceType.HIVE_STREAMING_DATABASE_NAME}").count()
          if(fist){
            val hasReportTable = sparkSession.sql(s"show tables  like '${ConstanceType.HIVE_STREAMING_TABLE_NAME}'").count()
            if(hasReportTable==0){
              fdataFrame.write.mode(SaveMode.Append).saveAsTable(s"${ConstanceType.HIVE_STREAMING_TABLE_NAME}")
            }else{
              fdataFrame.write.mode(SaveMode.Append).insertInto(s"${ConstanceType.HIVE_STREAMING_TABLE_NAME}")
            }
          }else{
            fdataFrame.write.mode(SaveMode.Append).insertInto(s"${ConstanceType.HIVE_STREAMING_TABLE_NAME}")
          }
        }catch {
          case e:Exception=>
            logError(e.getMessage,e)
            e.printStackTrace()
        }
      }
    }
  }
  def getSchema():StructType={
    val arrayStructField = new ArrayBuffer[StructField](8)
    //    bluesky_opts BIGINT,
    //    bluesky_start
    //    bluesky_sys BIGINT,
    //    data string
    //      tranceName string,
    //    timeConsuming int
    arrayStructField += StructField("bluesky_count",LongType , true)
    arrayStructField += StructField("bluesky_curr",LongType , true)
    arrayStructField += StructField("bluesky_opts",LongType , true)
    arrayStructField += StructField("bluesky_start",LongType , true)
    arrayStructField += StructField("bluesky_end",LongType , true)
    arrayStructField += StructField("data",StringType , true)
    arrayStructField += StructField("timeConsuming",LongType , true)
    arrayStructField += StructField("tranceName",StringType , true)
    StructType(arrayStructField)

  }
}
