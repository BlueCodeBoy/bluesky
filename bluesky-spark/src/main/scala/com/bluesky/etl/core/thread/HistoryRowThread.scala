package com.bluesky.etl.core.thread

import java.util.concurrent.TimeUnit

import com.bluesky.etl.core.enums.ConstanceType
import com.bluesky.etl.core.thread.`object`.HistoryRowObject
import com.bluesky.etl.core.trans.Trans
import com.bluesky.etl.utils.{HashUtils, Logging}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, _}
import org.pentaho.di.i18n.BaseMessages

import scala.util.control.Breaks._

/**
  * Created by root on 18-8-15.
  */
class HistoryRowThread(sparkSession: SparkSession, trans: Trans) extends Runnable  with Logging {
  val obj:Object = new Object()


  override def run(): Unit = {
    if(trans.getCanStoreHistore()) {
      logInfo(s"trance${trans.tranceName} start  storing history ")
      Thread.currentThread().setName("A:HistoryRowThread")
      val tableName:String = s"${trans.tranceId}_history"
      HistoryRowObject.checkDataBase(sparkSession)
      try {
        var dataFrame: DataFrame = null
        breakable {
//          while ((dataFrame = trans.realTimeHistoryRowSet.getRowBLock) != null) {
//                HistoryRowObject.saveDataToHive(dataFrame,tableName,sparkSession)
//          }
          while (true) {
            if(trans.stopped_){
              break()
            }
//            dataFrame = trans.realTimeHistoryRowSet.getRowWait(1,TimeUnit.MINUTES)
            dataFrame = trans.realTimeHistoryRowSet.getRowBLock
            if(null!=dataFrame){
              HistoryRowObject.saveDataToHive(dataFrame,tableName,sparkSession)
//            }else{
//              TimeUnit.MINUTES.sleep((new java.util.Random()).nextInt(10))
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
    }else{
      logInfo(s"tranceName (${trans.tranceName})  can not store history ")
    }

  }
}
