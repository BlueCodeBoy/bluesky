package com.bluesky.etl.steps.output.deleterow

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
//import org.pentaho.di.trans.deleterow.{DeleteRowData, DeleteRowMeta}
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.update.{UpdateData, UpdateMeta}

class DeleteRowSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
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
//    var deleteRowMeta: DeleteRowMeta = null
//    var deleteRowData: DeleteRowData = null
//    var objectMeta:DeleteRowObjectMeta = null
    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
      true
//        deleteRowMeta = smi.asInstanceOf[DeleteRowMeta]
//        deleteRowData = sdi.asInstanceOf[DeleteRowData]
//
//        val dataFrame: DataFrame = getData()
//        if (null == dataFrame) {
//            setOutputDone()
//            return false
//        }
//        try{
//          if(null != dataFrame){
//              val sourceFields = dataFrame.schema.map(d=>d.name)
//              if(null ==objectMeta) {
//                  objectMeta = new DeleteRowObjectMeta(deleteRowMeta.getDatabaseMeta.getUsername,
//                      deleteRowMeta.getDatabaseMeta.getPluginId,
//                      deleteRowMeta.getDatabaseMeta.getPassword
//                      , deleteRowMeta.getDatabaseMeta.getDriverClass,
//                      deleteRowMeta.getDatabaseMeta.getURL(),
//                      deleteRowMeta.getSchemaName,
//                      deleteRowMeta.getTableName,
//                      deleteRowMeta.getKeyStream.toList,
//                      deleteRowMeta.getKeyLookup.toList,
//                      deleteRowMeta.getKeyCondition.toList,
//                      deleteRowMeta.getKeyStream2.toList)
//              }
//              DeleteRowObject.deleteData(objectMeta,dataFrame)
//              putData(null,dataFrame)
//              }
//          if(!trans.getisStreaming()){
//              setStopped(true)
//          }
//        }catch {
//            case e:Exception=>{
//                putErrorData(e.getMessage,dataFrame)
//            }
//        }
//        true
    }


}
