package com.bluesky.etl.core.thread

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import com.bluesky.etl.core.enums.ConstanceType
import com.bluesky.etl.core.trans.Trans
import com.bluesky.etl.utils.{HashUtils, Logging}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.pentaho.di.i18n.BaseMessages

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by root on 18-8-15.
  */
class SaveErrorRowThread(sparkSession: SparkSession, trans: Trans) extends Runnable  with Logging {
  val obj:Object = new Object()
  /**
    * check database
    */
  def checkDataBase():Unit={
    val hasReportDatabase = sparkSession.sql(s"show databases  like '${ConstanceType.HIVE_STREAMING_DATABASE_NAME.toString}'").count()
    if (hasReportDatabase == 0) {
      sparkSession.sql(s"create database  ${ConstanceType.HIVE_STREAMING_DATABASE_NAME.toString}").count()
    }
  }

  /**
    *
    * @param dataFrame
    */
  def saveDataToHive(dataFrame: (String,String,String,DataFrame),tableName:String):Unit={
    obj.synchronized{
      sparkSession.sql(s"use ${ConstanceType.HIVE_STREAMING_DATABASE_NAME}").count()
      val fields = dataFrame._4.schema.fieldNames
      val hasField = fields.contains(ConstanceType.HIVE_STREAMING_ERROR_TABLE_TRY_TIME)
      val hasTable = sparkSession.sql(s"show tables  like '${tableName}'").count()
      if(hasField){
        val d = System.currentTimeMillis()
        val fdataFrame = dataFrame._4.withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_TRY_TIME.toString,
          col(ConstanceType.HIVE_STREAMING_ERROR_TABLE_TRY_TIME.toString)+1)
          .withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_ADD_TIME.toString,
            lit(d))
          .withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_ERROR_MSG.toString,
            lit(dataFrame._1))
        if(hasTable==0){
          fdataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)
        }else{
          fdataFrame.write.mode(SaveMode.Append).insertInto(tableName)
        }

      }else{
        //new  input data
        val d = System.currentTimeMillis()
        val fdataFrame = dataFrame._4.withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_TRY_TIME.toString,
          lit(0))
          .withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_ADD_TIME.toString,
            lit(d))
          .withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_ERROR_MSG.toString,
            lit(dataFrame._1))
          .withColumn(ConstanceType.HIVE_STREAMING_ERROR_TABLE_SEQUENCE.toString,
            monotonically_increasing_id)
        if(hasTable==0){
          fdataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)
        }else{
          fdataFrame.write.mode(SaveMode.Append).insertInto(tableName)
        }
      }
    }
  }
  override def run(): Unit = {
    Thread.currentThread().setName("A:SaveErrorRowThread")
    var first:Boolean = true
    var tableName:String = null
    try {
      var dataFrame:(String,String,String,DataFrame) = null
      breakable {
//        while ((dataFrame = trans.realTimeErrorRowSet.getErrorRowLock)!=null){
//          if(dataFrame==null){
//            break()
//          }else{
//            if(first){
//              checkDataBase()
//            }
//            tableName = s"${dataFrame._2}_${trans.tranceId}_${HashUtils.Md5hash(dataFrame._3)}"
//            saveDataToHive(dataFrame,tableName)
//            if(first) {
//              first = false
//            }
//          }
//        }
        while (true){
//          dataFrame = trans.realTimeErrorRowSet.getErrorRowWait(3,TimeUnit.MINUTES)
          dataFrame = trans.realTimeErrorRowSet.getErrorRowLock
          if(trans.stopped_) {
            break()
          }
          if(null != dataFrame){
            if(!dataFrame._4.rdd.isEmpty()) {
              if (first) {
                checkDataBase()
              }
              tableName = s"${dataFrame._2}_${trans.tranceId}_${HashUtils.Md5hash(dataFrame._3)}"
              saveDataToHive(dataFrame, tableName)
              if (first) {
                first = false
              }
            }
//          }else{
//            TimeUnit.MINUTES.sleep((new java.util.Random()).nextInt(10))
          }else{
            break()
          }
        }
      }
    } catch {
      case t: Exception =>
        t.printStackTrace()
        try { // check for OOME
          if (t.isInstanceOf[OutOfMemoryError]) { // Handle this different with as less overhead as possible to get an error message in the log.
            logError("UnexpectedError: ", t)
          }
          else {
            t.printStackTrace()
            logError(BaseMessages.getString("System.Log.UnexpectedError"), t)
          }
        } catch {
          case e: OutOfMemoryError =>
            e.printStackTrace()
        } finally {
        }
    } finally {
      logInfo("saveErrorRow thread is shutdown")
    }

  }
}
