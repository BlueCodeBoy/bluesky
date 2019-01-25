package com.bluesky.etl.core.thread

import com.bluesky.etl.core.enums.ConstanceType
import com.bluesky.etl.core.trans.Trans
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql._
import org.pentaho.di.trans.TransMeta

/**
  * Created by root on 18-8-15.
  */
class RestoreErrorRowThread(sparkSession: SparkSession, trans: Trans,transMeta: TransMeta) extends Runnable  with Logging {
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
    * @param tableName
    * @return
    */
  def restoreDataFromHive(tableName:String,tryTime:Int):DataFrame={
    obj.synchronized {
      sparkSession.sql(s"use ${ConstanceType.HIVE_STREAMING_DATABASE_NAME}").count()
      val hasTable = sparkSession.sql(s"show tables  like '${tableName}'").count()
      if(hasTable!=0){
        sparkSession.sql(s"select *  from  (select row_number() over(partition by ${ConstanceType.HIVE_STREAMING_ERROR_TABLE_SEQUENCE.toString} order by ${ConstanceType.HIVE_STREAMING_ERROR_TABLE_ADD_TIME.toString} desc)  as v ,* from ${tableName}) as tm where tm.v=1 and ${ConstanceType.HIVE_STREAMING_ERROR_TABLE_TRY_TIME.toString}=${tryTime} order by ${ConstanceType.HIVE_STREAMING_BLUESKY_TIMESTAMP.toString} asc")
      }else{
        null
      }
    }
  }
  override def run(): Unit = {
    Thread.currentThread().setName("A:RestoreErrorRowThread")
//    var tableName:String = null
//    var idName:(String,String) = null
//    var tmpDataFrame:DataFrame = null
//    var tryTime:Int = 0
//    try {
//      breakable {
//        while (true){
//          tryTime = 0
//          TimeUnit.MINUTES.sleep((new java.util.Random()).nextInt(10))
//          if(trans.stopped_){
//            break()
//          }
//          for (stm <- trans.queryAllStep()) {
//            logInfo(s"starting restore the step: ${stm.stepname}")
//            idName = (stm.stepMeta.getStepID,stm.stepname)
//            tableName = s"${idName._1}_${trans.tranceId}_${HashUtils.Md5hash(idName._2)}"
//            tmpDataFrame = restoreDataFromHive(tableName,tryTime)
//            if(null!=tmpDataFrame &&Try(tmpDataFrame.first()).isSuccess){
//              logInfo(s"the step:${stm.stepname} data is not empty")
//              stm.step.restoreData(tmpDataFrame)
//              sparkSession.sql(s"TRUNCATE TABLE ${tableName}")
//              logInfo(s"the step:${stm.stepname} restore is success")
//            }else{
//              logInfo(s"the step:${stm.stepname} data is empty")
//            }
//            logInfo(s"end restore the step:${stm.stepname}")
//          }
//          tryTime+=1
//          if(tryTime>=3){
//            tryTime=0
//          }
//        }
//      }
//    } catch {
//      case t: Exception =>
//        t.printStackTrace()
//        try { // check for OOME
//          if (t.isInstanceOf[OutOfMemoryError]) { // Handle this different with as less overhead as possible to get an error message in the log.
//            logError("UnexpectedError: ", t)
//          }
//          else {
//            t.printStackTrace()
//            logError(BaseMessages.getString("System.Log.UnexpectedError"), t)
//          }
//        } catch {
//          case e: OutOfMemoryError =>
//            e.printStackTrace()
//        } finally {
//        }
//    } finally {
//      logInfo("restoreErrorRow thread is shutdown")
//    }

  }
}
