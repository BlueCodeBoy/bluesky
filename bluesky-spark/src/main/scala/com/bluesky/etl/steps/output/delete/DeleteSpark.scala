package com.bluesky.etl.steps.output.deleterow

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.delete.{DeleteData, DeleteMeta}
import org.pentaho.di.trans.steps.update.{UpdateData, UpdateMeta}

class DeleteSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                  trans: Trans, stepDataInterface: StepDataInterface, baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
    trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface {

    var deleteMeta: DeleteMeta = null
    var deleteData: DeleteData = null
    var objectMeta:DeleteObjectMeta = null
  /**
    *
    * @param data
    */
  override def restoreData(data: Dataset[Row]): Unit = {
    putDataToInputRowSet(data)
     restoreObject.synchronized{      restoreObject.wait()    }
  }
    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
        deleteMeta = smi.asInstanceOf[DeleteMeta]
        deleteData = sdi.asInstanceOf[DeleteData]

        val dataFrame: DataFrame = getData()
        if (null == dataFrame) {
            setOutputDone()
            return false
        }
        try{
          if(null != dataFrame){
              val sourceFields = dataFrame.schema.map(d=>d.name)
              if(null == objectMeta) {
                objectMeta = new DeleteObjectMeta(deleteMeta.getDatabaseMeta.getUsername,
                  deleteMeta.getDatabaseMeta.getPluginId,
                  deleteMeta.getDatabaseMeta.getPassword
                  , deleteMeta.getDatabaseMeta.getDriverClass,
                  deleteMeta.getDatabaseMeta.getURL(),
                  deleteMeta.getSchemaName,
                  deleteMeta.getTableName,
                  deleteMeta.getKeyStream.toList,
                  deleteMeta.getKeyLookup.toList,
                  deleteMeta.getKeyCondition.toList,
                  deleteMeta.getKeyStream2.toList,
                  deleteMeta.getCommitSize
                )
              }
            val startTime = System.currentTimeMillis()
              DeleteObject.deleteData(objectMeta,dataFrame)
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
