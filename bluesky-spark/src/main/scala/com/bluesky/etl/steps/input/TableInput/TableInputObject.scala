package com.bluesky.etl.steps.input.TableInput

import java.util

import com.bluesky.etl.core.enums.CommonFieldType
import com.bluesky.etl.core.trans.BaseMeta
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.pentaho.di.core.database.DatabaseMeta
import org.pentaho.di.core.util.StringUtil

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try

class TableInputMetaObject(val tableName:String,val sql:String,val databaseMeta:DatabaseMeta,
                           val rowLimit:String,
                           val  patitionColum:String,
                           val numPartition:Int
) extends BaseMeta with Serializable{

}
/**
  * Created by root on 18-8-16.
  */
case object TableInputObject extends Logging{

  def loadData(pluginId:String,sparkSession: SparkSession,batchNumber:Long,
               tableInputObject: TableInputMetaObject,
               attrMap:util.Map[String, util.Map[String, String]]):DataFrame={
    val fields = tableInputObject.sql.replaceAll(s"FROM ${tableInputObject.tableName}","").replaceFirst("SELECT","").replaceFirst("select","").replaceAll(" ","").replaceAll("\\n","").split(",")
    var data: DataFrame = null
    pluginId match {
      case "Hive2Shim" =>
        data = sparkSession.sql(tableInputObject.sql)
      case "MYSQL" =>
        val tmpDataFrame = sparkSession.read.format("jdbc").options(
          Map(
            "url" -> tableInputObject.databaseMeta.getURL,
            "user" -> tableInputObject.databaseMeta.getUsername,
            "dbtable" -> tableInputObject.tableName,
            "password" ->tableInputObject.databaseMeta.getPassword,
            "driver" -> tableInputObject.databaseMeta.getDriverClass))

        tmpDataFrame.load().createOrReplaceTempView("tmp_tableintput")
        val maxMinData = sparkSession.sql(s"select max(${tableInputObject.patitionColum}) as max,min(${tableInputObject.patitionColum}) as min from tmp_tableintput ").collect()
        if(maxMinData.isEmpty){
         throw  new RuntimeException("max min is null")
        }
        val max = maxMinData.apply(0).getInt(0)
        val min = maxMinData.apply(0).getInt(1)
        data = tmpDataFrame.option("columnName",tableInputObject.patitionColum).
          option("lowerBound",min).
          option("upperBound",max).
          option("numPartitions",tableInputObject.numPartition).load()
      case "ORACLE" =>
        val tmpDataFrame = sparkSession.read.format("jdbc").options(
          Map(
            "url" -> tableInputObject.databaseMeta.getURL,
            "user" -> tableInputObject.databaseMeta.getUsername,
            "dbtable" -> tableInputObject.tableName,
            "password" ->tableInputObject.databaseMeta.getPassword,
            "driver" -> tableInputObject.databaseMeta.getDriverClass))
        val tmpTableName = s"${tableInputObject.tableName.replaceAll("\\.","")}_tmp_tableintput"
        tmpDataFrame.load().createOrReplaceTempView(tmpTableName)
        val maxMinData = sparkSession.sql(s"select max(${tableInputObject.patitionColum}) as max,min(${tableInputObject.patitionColum}) as min from ${tmpTableName} ").collect()
        if(maxMinData.isEmpty){
         throw  new RuntimeException("max min is null")
        }
        val max = maxMinData.apply(0).getAs("max").toString.toInt
        val min = maxMinData.apply(0).getAs("min").toString.toInt
        data = tmpDataFrame.option("columnName",tableInputObject.patitionColum).
          option("lowerBound",min).
          option("upperBound",max).
          option("numPartitions",tableInputObject.numPartition).load()
       case _ =>
        throw new RuntimeException("not support")
    }
    if(null!=data){
      val rowLimt = tableInputObject.rowLimit.toInt
      if(rowLimt>0){
        data = data.limit(rowLimt)
      }
      data = data.withColumn(CommonFieldType.DATAFRAME_POSTFIX.toString,lit("")).
        withColumn(CommonFieldType.SF_BATCH.toString,lit(batchNumber)).
        withColumn(CommonFieldType.SF_RES.toString,lit(0)).select(fields.map(col):_*)
      data
    }else{
      null
    }
  }


}
