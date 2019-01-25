package com.bluesky.etl.steps.output.tableoutput

import com.alibaba.druid.pool.DruidDataSource
import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.tableoutput.{TableOutputData, TableOutputMeta}
import org.pentaho.di.trans.steps.update.{UpdateData, UpdateMeta}

class TableOutputSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                       trans: Trans, stepDataInterface: StepDataInterface, baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
    trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface {
  /**
    *
    * @param data
    */
  override def restoreData(data: Dataset[Row]): Unit = {
    putDataToInputRowSet(data)
       restoreObject.synchronized{      restoreObject.wait()    }

  }

    var tableOutputMeta: TableOutputMeta = null
    var tableOutputData: TableOutputData = null
    var tableOutputObjectMeta:TableOutputObjectMeta = null

    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
        tableOutputMeta = smi.asInstanceOf[TableOutputMeta]
        tableOutputData = sdi.asInstanceOf[TableOutputData]

        val dataFrame: DataFrame = getData()
        if (null == dataFrame) {
            setOutputDone()
            return false
        }
        try{
          if(null != dataFrame){
            if(null==tableOutputObjectMeta){
              tableOutputObjectMeta = new TableOutputObjectMeta(
                tableOutputMeta.getDatabaseMeta.getUsername,tableOutputMeta.getDatabaseMeta.getPluginId,tableOutputMeta.getDatabaseMeta.getPassword,tableOutputMeta.getDatabaseMeta.getDriverClass,tableOutputMeta.getDatabaseMeta.getURL(),
                tableOutputMeta.getSchemaName,tableOutputMeta.getTableName,tableOutputMeta.getCommitSize.toString.toInt,tableOutputMeta.truncateTable(),tableOutputMeta.ignoreErrors(),tableOutputMeta.useBatchUpdate(),tableOutputMeta.isTableNameInField,tableOutputMeta.getTableNameField,tableOutputMeta.isTableNameInTable,tableOutputMeta.isReturningGeneratedKeys,tableOutputMeta.getGeneratedKeyField,tableOutputMeta.specifyFields(),tableOutputMeta.getFieldStream.toList,tableOutputMeta.getFieldDatabase.toList)
            }
            val startTime = System.currentTimeMillis()
            TableOutputObject.startSaveData(tableOutputObjectMeta,dataFrame,stepName)
            putData(null,dataFrame)
            //statistics row
            val endTime = System.currentTimeMillis()
            trans.realTimeErrorRowSet.putRowWithTime(startTime,endTime,null,dataFrame)
          }
          if(!trans.getisStreaming()){
              setStopped(true)
          }
        }catch {
            case e:Exception=>{
              logError(e.getMessage,e)
                putErrorData(e.getMessage,dataFrame)
            }
        }
        true
    }



}
