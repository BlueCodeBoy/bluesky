package com.bluesky.etl.core.thread

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.bluesky.etl.core.enums.ConstanceType
import com.bluesky.etl.core.trans.{SparkStepInterface, Trans}
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.pentaho.di.i18n.BaseMessages

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.Breaks._

/**
  * Created by root on 18-8-15.
  */
class StatisticRowThread(sparkSession: SparkSession,trans: Trans) extends Runnable  with Logging   {

  override def run(): Unit = {
    Thread.currentThread().setName("A:StatisticRowThread")
    var first:Boolean = true
    try {
      var dataFrame:(Long,Long,DataFrame) = null
      breakable {
//        while ((dataFrame = trans.realTimeErrorRowSet.getRowWithTimeLock)!=null){
//          if(dataFrame==null){
//            break()
//          }else{
//            if(first){
//              StatisticRowObject.checkDataBase(dataFrame,sparkSession)
//            }
//            StatisticRowObject.saveDataToHive(dataFrame,first,sparkSession,trans.tranceName)
//            if(first){
//              first = false
//            }
//          }
//        }
        while (true){
//        dataFrame = trans.realTimeErrorRowSet.getRowWithTimeWait(30000,TimeUnit.MILLISECONDS)
          dataFrame = trans.realTimeErrorRowSet.getRowWithTimeLock
          if(trans.stopped_){
            break()
          }
          try {
            if (null != dataFrame  ) {
              if(!dataFrame._3.rdd.isEmpty()){
                if (first) {
                  StatisticRowObject.checkDataBase(dataFrame, sparkSession)
                }
                StatisticRowObject.saveDataToHive(dataFrame, first, sparkSession, trans.tranceName)
                if (first) {
                  first = false
                }
              }

//            } else {
//              TimeUnit.MINUTES.sleep((new java.util.Random()).nextInt(10))
            }else{
              break()
            }
          }catch {
            case e:Exception=>
              logError(s"statisticRowThread error${e.getMessage}")
          }finally {
            dataFrame = null
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
      logInfo("statisticRow thread is shutdown")
    }

  }
}
