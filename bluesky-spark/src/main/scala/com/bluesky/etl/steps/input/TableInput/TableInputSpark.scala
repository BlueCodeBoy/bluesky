package com.bluesky.etl.steps.input.TableInput


import java.util.concurrent.TimeUnit

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.tableinputspark.{TableInputDataForSpark, TableInputMetaForSpark}

import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
/**
  * Created by root on 18-8-14.
  */
class TableInputSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                      trans: Trans, stepDataInterface: StepDataInterface, baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
  trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface with Serializable{
  private var tableInputMeta: TableInputMetaForSpark = null
  private var tableInputData: TableInputDataForSpark = null
  private var sb:(StructType,Broadcast[List[Row]]) = null
  var processTime:Int = 0
  var df:DataFrame = null
  /**
    *
    * @param data
    */
  override def restoreData(data: Dataset[Row]): Unit = {
    putDataToInputRowSet(data)
     restoreObject.synchronized{      restoreObject.wait()    }
  }
  override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
    tableInputMeta = smi.asInstanceOf[TableInputMetaForSpark]
    tableInputData = sdi.asInstanceOf[TableInputDataForSpark]
    if(null == sb){
      val tableInputMetaObject = new TableInputMetaObject(tableInputMeta.getTableName,
        tableInputMeta.getSQL,tableInputMeta.getDatabaseMeta,
        tableInputMeta.getRowLimit,tableInputMeta.getPrimaryKey,tableInputMeta.getNumParttion.toString.toInt)
      df = TableInputObject.loadData(tableInputMetaObject.databaseMeta.getDatabaseInterface.getPluginId
          ,sparkSession,trans.getbatchNumber(),tableInputMetaObject,trans.getAttributesMap)
      val data = df.collect().toList
      logInfo(s"The broadCast DATA size:is ${df.collect().toList.toString().getBytes()} bytes")
        sb = (df.schema,sparkSession.sparkContext.broadcast(data))
      putBroadCastData(tableInputData.rowMeta,sb)
    }else{
      if(null!=df && !trans.getisStreaming()){
        putData(null,df)
      }
      processTime+=1
      TimeUnit.MINUTES.sleep(processTime*1)
    }
    true
  }
}
