package com.bluesky.etl.core.trans

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bluesky.etl.core.entity.RuleDbInfoVo
import com.bluesky.etl.core.enums.{CommonFieldType, CommonStepId, ConstanceType}
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.sql.functions.col

import scala.util.Try

/**
  * Created by root on 18-8-30.
  */
case object DataMoveStatisticsObject extends Logging{
  /**
    *搬移数据
    * @param sparkSession
    * @param dataFrame
    * @param isIncrement
    * @param pk
    * @param moveErrorDataEnable
    * @param moveRightDataEnable
    * @param errorRuleDbinfoVo
    * @param errorTable
    * @param rightRuleDbinfoVo
    * @param successTable
    */
  def moveData(sparkSession: SparkSession, dataFrame: DataFrame,isIncrement:Boolean,
               pk:String, moveErrorDataEnable:Boolean, moveRightDataEnable:Boolean,
               errorRuleDbinfoVo:RuleDbInfoVo, errorTable:String,
               rightRuleDbinfoVo:RuleDbInfoVo,successTable:String
              ):Unit={

  val filedsList = dataFrame.schema.fields.map(data=>data.name.toString).toList diff List(CommonFieldType.SF_RES.toString,
    CommonFieldType.SF_BATCH.toString,
    CommonFieldType.DATAFRAME_POSTFIX.toString)
    val NoResfiledsList = dataFrame.schema.fields.map(data=>data.name.toString).toList diff List(CommonFieldType.SF_RES.toString,
//      CommonFieldType.SF_BATCH.toString,
      CommonFieldType.DATAFRAME_POSTFIX.toString)
  if(pk==null){
    throw new RuntimeException("pk is null (id not found)")
  }
  //move error data
  if(moveErrorDataEnable){
    if(errorRuleDbinfoVo!=null) {
      val errorDataFrame = dataFrame.filter(s"${CommonFieldType.SF_RES.toString}>0").select(NoResfiledsList.map(col):_*)

//      val errorTableName =  s"${errorRuleDbinfoVo.getDbName}.${errorTable}"
      val tmperrorTable = s"${errorTable}tmp"
      errorRuleDbinfoVo.getDbType match {
        case "HIVE" =>
          val hasErrorDatabase = sparkSession.sql(s"show databases  like '${errorRuleDbinfoVo.getDbName}'").count()
          if (hasErrorDatabase == 0) {
            sparkSession.sql(s"create database  ${errorRuleDbinfoVo.getDbName}").count()
          }
          sparkSession.sql(s"use ${errorRuleDbinfoVo.getDbName}").count()
          val hasErrorTable = sparkSession.sql(s"show tables  like '${errorTable}'").count()
          logInfo(s"isIncrement:${isIncrement},hasErrorTable:${hasErrorTable}")
          //if isIncrement----
          if(isIncrement&&hasErrorTable!=0){
            logInfo(s"move error data:isIncrement=true union-merge table  and hasErrorTable: ${hasErrorTable}: ")
            val originalErrorDataFrame = sparkSession.sql(s"select * from $errorTable")
            if(!Try(originalErrorDataFrame.first()).isSuccess){
              logInfo(s"table $errorTable :originalErrorDataFrame is not  null")
              if(hasErrorTable==0){
                errorDataFrame.select(filedsList.map(col):_*).write
                  .mode(SaveMode.Append).saveAsTable(errorTable)
              }else{
                errorDataFrame.select(filedsList.map(col):_*)
                  .write.mode(SaveMode.Append).insertInto(errorTable)
              }
            }else{
              logInfo(s"table $errorTable :originalErrorDataFrame is not  null")
              val mergeData = originalErrorDataFrame.withColumn(CommonFieldType.SF_BATCH.toString,lit(0)).union(errorDataFrame)
              mergeData.createOrReplaceTempView("SF"+errorTable)

              sparkSession.sql(s"DROP TABLE IF EXISTS ${tmperrorTable}").count()
              sparkSession.sql("select " + filedsList.mkString(",") + s" from (select row_number() over(partition by $pk order by ${CommonFieldType.SF_BATCH.toString} desc)  as v ,* from SF$errorTable) as tm where tm.v=1")
                .write.mode(SaveMode.Overwrite).saveAsTable(tmperrorTable)
              sparkSession.sql(s"DROP TABLE IF EXISTS ${errorTable}").count()
              sparkSession.sql(s"ALTER TABLE ${tmperrorTable} RENAME TO ${errorTable}").count()
              sparkSession.sql(s"DROP TABLE IF EXISTS ${tmperrorTable}").count()

            }
          }else{
            logInfo(s"move error data:isIncrement=false Overwrite table or hasErrorTable:${hasErrorTable}")
            if(hasErrorTable==0){
              errorDataFrame.select(filedsList.map(col): _*).write.mode(SaveMode.Overwrite).saveAsTable(errorTable)
            }else{
              errorDataFrame.select(filedsList.map(col): _*).write.mode(SaveMode.Overwrite).insertInto(errorTable)
            }
          }
        case _ =>
          logInfo(s"move error data has not support db type :${errorRuleDbinfoVo.getDbType}")
      }
    }else{
      logInfo("getMoveErrorDataEnable is true but getErrorRuleDbInfoVo is null(posssiable quality url is not defined or quality web application is deaded)")
    }

  }else{
    logInfo("Not configured getMoveErrorDataEnable")
  }
  //move success data
  if(moveRightDataEnable){
    if(rightRuleDbinfoVo!=null){
      val successDataFrame = dataFrame.filter(s"${CommonFieldType.SF_RES.toString}=0").select(NoResfiledsList.map(col):_*)
//      val successTableName =  s"${rightRuleDbinfoVo.getDbName}.${successTable}"
      val tmpsuccessTable = s"${successTable}tmp"
      rightRuleDbinfoVo.getDbType match {
        case "HIVE"=>
          val hasRightDatabase = sparkSession.sql("show databases  like '"+rightRuleDbinfoVo.getDbName+"'").count()
          if(hasRightDatabase==0){
            sparkSession.sql(s"create database  ${rightRuleDbinfoVo.getDbName}").count()
          }
          sparkSession.sql(s"use  ${rightRuleDbinfoVo.getDbName}").count()
          val hasRightTable = sparkSession.sql(s"show tables  like '${successTable}'").count()
          //if isIncrement----
          logInfo(s"isIncrement:${isIncrement},hasRightTable:${hasRightTable}")
          if(isIncrement&&hasRightTable!=0) {
            logInfo(s"move success data:isIncrement=true union-merge table and hasRightTable ${hasRightTable}")
            val originalRightDataFrame = sparkSession.sql(s"select * from $successTable")
            if (!Try(originalRightDataFrame.first()).isSuccess) {
              logInfo(s"table $successTable :originalRightDataFrame is null")
              if(hasRightTable==0){
                successDataFrame.select(filedsList.map(col):_*).write
                  .mode(SaveMode.Append).saveAsTable(successTable)
              }else{
                successDataFrame.select(filedsList.map(col):_*).write
                  .mode(SaveMode.Append).insertInto(successTable)
              }
            } else {
              logInfo(s"table $successTable :originalRightDataFrame is not null")
              val mergeData = originalRightDataFrame.withColumn(CommonFieldType.SF_BATCH.toString,lit(0)).union(successDataFrame)
              mergeData.createOrReplaceTempView("SF" + successTable)

              sparkSession.sql(s"DROP TABLE IF EXISTS ${tmpsuccessTable}").count()

              sparkSession.sql("select " + filedsList.mkString(",") + s" from (select row_number() over(partition by $pk order by ${CommonFieldType.SF_BATCH.toString} desc)  as v ,* from SF$successTable) as tm where tm.v=1")
                .write.mode(SaveMode.Overwrite).saveAsTable(tmpsuccessTable)

              sparkSession.sql(s"DROP TABLE IF EXISTS ${successTable}").count()
              sparkSession.sql(s"ALTER TABLE ${tmpsuccessTable} RENAME TO ${successTable}").count()
              sparkSession.sql(s"DROP TABLE IF EXISTS ${tmpsuccessTable}").count()
            }
          }else{
            logInfo(s"move success data:isIncrement=false Overwrite table or ${hasRightTable}:is 0")
            if(hasRightTable==0){
              successDataFrame.select(filedsList.map(col): _*).write.mode(SaveMode.Overwrite).saveAsTable(successTable)
            }else{
              successDataFrame.select(filedsList.map(col): _*).write.mode(SaveMode.Overwrite).insertInto(successTable)
            }
          }
        case _=>
          logInfo(s"move right data has not support db type :${rightRuleDbinfoVo.getDbType}" )
      }
    }else{
      logInfo("getRightRuleDbInfoVo is true but getRightRuleDbInfoVo is null(posssiable quality url is not defined or quality web application is deaded)")
    }
  }else{
    logInfo("Not configured getMoveRightDataEnable")
  }
  }

  /**
    *统计数据
    * @param sparkSession
    * @param dataFrame
    * @param isIncrement
    */
  def statisticsData(sparkSession: SparkSession,dataFrame: DataFrame,
                   isIncrement:Boolean,reportTable:String,pk:String):Unit={
    if(pk==null){
      throw new RuntimeException("pk is null (id not found)")
    }
    val post = CommonStepId.values.map(data=>data.toString).toList
    val defineFieldTotal =
      List("completionTotal","accuracyTotal","rationalityTotal","consistencyTotal","uniqueTotal")
    val hasReportDatabase = sparkSession.sql(s"show databases  like '${ConstanceType.HIVE_REPORT_DATABASE_NAME.toString}'").count()
    if (hasReportDatabase == 0) {
      sparkSession.sql(s"create database  ${ConstanceType.HIVE_REPORT_DATABASE_NAME.toString}").count()
    }
    sparkSession.sql(s"use ${ConstanceType.HIVE_REPORT_DATABASE_NAME}").count()
//    val reportTableName =s"${ConstanceType.HIVE_REPORT_DATABASE_NAME.toString}.${reportTable}"
//    val tmpreportTableName =s"${ConstanceType.HIVE_REPORT_DATABASE_NAME.toString}.${reportTable}tmp"
    val tmpreportTable = s"${reportTable}_tmp"
    val hasReportTable = sparkSession.sql(s"show tables  like '${reportTable}'").count()
    val mapData = Seq("completionTotal"->List(CommonStepId.SFFieldDefect.toString,CommonStepId.CountRecords.toString),
      "accuracyTotal"->List(CommonStepId.SFFieldComparision.toString,CommonStepId.SFFieldInterval.toString),
      "rationalityTotal"->List(CommonStepId.SFFieldRegular.toString,CommonStepId.SFFieldEnum.toString,CommonStepId.FieldFormatVerify.toString,CommonStepId.DigitalPrecision.toString),
      "consistencyTotal"->List(CommonStepId.ForeignToPrimary.toString),
      "uniqueTotal"->List(CommonStepId.SFFieldUnique.toString))
    //filter SF_RES gt 0
    val tmpDataFrame = dataFrame.filter(s"${CommonFieldType.SF_RES.toString}>0").rdd.map(row=>{
//      filter error data
//      if(row.getAs[Int](CommonFieldType.SF_RES.toString)>0){
        val s = ListBuffer.empty ++= row.toSeq.toList
        val sf_col = s(row.fieldIndex(CommonFieldType.DATAFRAME_POSTFIX.toString))
        var jsonObject: JSONObject = null
        if (null == sf_col || "" == sf_col) {
          jsonObject = new JSONObject
        } else {
          jsonObject = JSON.parseObject(sf_col.toString)
        }
        for(p<-post){
          if(jsonObject.get(p)==null){
            s+=0
          }else{
            val objMap = jsonObject.get(p).asInstanceOf[java.util.Map[String,Boolean]]
            s+=objMap.values().toArray.map(da=>{if(da.asInstanceOf[Boolean]) 0 else 1}).reduce(_+_)
          }
        }
        val length = row.length
        for(x<-mapData.toArray){
          val count = x._2.map(data=>{
            s(post.indexOf(data)+length).asInstanceOf[Int]
          }).reduce(_+_)
          s+=count
        }
        Row.fromSeq(s)
//      }else{
//        Row.empty
//      }
    })
    val finalDataFrame =  sparkSession.createDataFrame(tmpDataFrame, StructType(getStructFieldListStatstic(post union defineFieldTotal,dataFrame.schema)))
    val filedsList = finalDataFrame.schema.fields.map(data=>data.name.toString).toList
    if(Try(finalDataFrame.first()).isSuccess){
      logInfo("starting statisiticsed")
      logInfo(s"isIncrement:${isIncrement},hasReportTable:${hasReportTable}")
      if(isIncrement&&hasReportTable!=0){
        logInfo("report data is  increment and report table exists")
        val originalRightDataFrame = sparkSession.sql(s"select * from $reportTable")
        if (!Try(originalRightDataFrame.first()).isSuccess) {
          logInfo(s"table $reportTable :reportdata is null")
          if(hasReportTable==0){
            finalDataFrame.write
              .mode(SaveMode.Append).saveAsTable(reportTable)
          }else{
            finalDataFrame.write
              .mode(SaveMode.Append).insertInto(reportTable)
          }
        } else {
          logInfo(s"table $reportTable :report is not null")
          val mergeData = originalRightDataFrame.union(finalDataFrame)
          mergeData.createOrReplaceTempView(s"SF$reportTable")
          sparkSession.sql(s"DROP TABLE IF EXISTS ${tmpreportTable}").count()
          sparkSession.sql(s"select *  from  (select row_number() over(partition by $pk order by ${CommonFieldType.SF_BATCH.toString} desc)  as v ,* from SF$reportTable) as tm where tm.v=1").
            select(filedsList.map(col): _*).write.
            mode(SaveMode.Overwrite).saveAsTable(tmpreportTable)
          sparkSession.sql(s"DROP TABLE IF EXISTS ${reportTable}").count()
          sparkSession.sql(s"ALTER TABLE ${tmpreportTable} RENAME TO ${reportTable}").count()
          sparkSession.sql(s"DROP TABLE IF EXISTS ${tmpreportTable}").count()
        }
      }else{
        logInfo("report data is no increment or report table not exsits")
        if(hasReportTable==0){
          finalDataFrame.write.mode(SaveMode.Overwrite).saveAsTable(reportTable)
        }else{
          finalDataFrame.write.mode(SaveMode.Overwrite).insertInto(reportTable)
        }
      }
    }else{
      logInfo("no error can be statisitics")
    }
  }

  /**
    *获取表结构
    * @param dictdata
    * @param rowtype
    * @return
    */
  def getStructFieldListStatstic(dictdata: List[String],rowtype:StructType): ArrayBuffer[StructField] ={
    val arrayStructField = new ArrayBuffer[StructField]()
    for (typeField <- rowtype.fields) {
      arrayStructField += StructField(typeField.name, typeField.dataType, true)
    }
    for (typeField <- dictdata) {
        arrayStructField += StructField(typeField, IntegerType, true)
    }
    arrayStructField
  }
}
