package com.bluesky.etl.core.thread.`object`


import com.bluesky.etl.core.enums.ConstanceType
import org.apache.spark.sql.{DataFrame,  SaveMode, SparkSession}


/**
  * Created by root on 18-11-14.
  */
case object HistoryRowObject {
  val obj:Object = new Object()

  /**
    *
    * @param sparkSession
    */
  def checkDataBase(sparkSession: SparkSession):Unit= {
    val hasReportDatabase = sparkSession.sql(s"show databases  like '${ConstanceType.HIVE_STREAMING_DATABASE_NAME.toString}'").count()
    if (hasReportDatabase == 0) {
      sparkSession.sql(s"create database  ${ConstanceType.HIVE_STREAMING_DATABASE_NAME.toString}").count()
    }
  }

  /**
    *
    * @param dataFrame
    * @param tableName
    * @param sparkSession
    */
  def saveDataToHive(dataFrame:DataFrame, tableName:String,sparkSession: SparkSession):Unit={
    obj.synchronized{
      sparkSession.sql(s"use ${ConstanceType.HIVE_STREAMING_DATABASE_NAME}").count()
      val hasHistoryTable = sparkSession.sql(s"show tables  like '${tableName}'").count()
      if(hasHistoryTable==0){
        dataFrame.write.mode(SaveMode.Append).saveAsTable(s"${tableName}")
      }else{
        dataFrame.write.mode(SaveMode.Append).insertInto(s"${tableName}")
      }
    }
  }
}
