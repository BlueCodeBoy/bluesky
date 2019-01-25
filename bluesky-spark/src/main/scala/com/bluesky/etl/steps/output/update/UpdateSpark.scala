package com.bluesky.etl.steps.output.update

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import com.bluesky.etl.steps.selectvalues.SelectValuesObject.{SelectFieldSpark, SelectMetadataChangeSpark, SelectValuesObjectMeta}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.selectvalues.{SelectValuesData, SelectValuesMeta}
import org.pentaho.di.trans.steps.update.{UpdateData, UpdateMeta}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class UpdateSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
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
    var updateMeta: UpdateMeta = null
    var updateData: UpdateData = null
    var objectMeta:UpdateObjectMeta = null

    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
        updateMeta = smi.asInstanceOf[UpdateMeta]
        updateData = sdi.asInstanceOf[UpdateData]

        val dataFrame: DataFrame = getData()
        if (null == dataFrame) {
            setOutputDone()
            return false
        }
        try{
          if(null != dataFrame){
              val sourceFields = dataFrame.schema.map(d=>d.name)
              val (updateFieldsDB,updateFieldsStream) = (updateMeta.getUpdateLookup.toBuffer,updateMeta.getUpdateStream.toBuffer)
              val (updateFieldsDBreal,updateFieldsStreamreal) = (updateMeta.getUpdateLookup.toBuffer,updateMeta.getUpdateStream.toBuffer)
              for(index <- 0 until updateFieldsDB.length){
                  if(sourceFields.indexOf(updateFieldsStream.apply(index)) == -1){
                      updateFieldsDBreal.remove(index)
                      updateFieldsStreamreal.remove(index)
                  }
              }
              if(null == objectMeta) {
                objectMeta = new UpdateObjectMeta(updateMeta.getDatabaseMeta.getUsername, updateMeta.getDatabaseMeta.getPluginId, updateMeta.getDatabaseMeta.getPassword, updateMeta.getDatabaseMeta.getDriverClass, updateMeta.getDatabaseMeta.getURL(),
                  updateMeta.getSchemaName, updateMeta.getTableName, updateMeta.getKeyStream.toList, updateMeta.getKeyLookup.toList, updateMeta.getKeyCondition.toList, updateMeta.getKeyStream2.toList,
                  updateFieldsDBreal.toList, updateFieldsStreamreal.toList,
                  updateMeta.getCommitSize, updateMeta.isErrorIgnored, updateMeta.getIgnoreFlagField, updateMeta.isSkipLookup, updateMeta.useBatchUpdate())
              }
              if(!objectMeta.updateLookup.isEmpty){
                val startTime = System.currentTimeMillis()
                UpdateObject.updateData(objectMeta,dataFrame,stepName)
                //statistics row
                trans.realTimeErrorRowSet.putRowWithTime(startTime,System.currentTimeMillis(),null,dataFrame)
              }
              putData(null,dataFrame)
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
