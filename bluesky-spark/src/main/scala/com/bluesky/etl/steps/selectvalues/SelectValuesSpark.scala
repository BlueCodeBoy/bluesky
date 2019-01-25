package com.bluesky.etl.steps.selectvalues

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import com.bluesky.etl.steps.selectvalues.SelectValuesObject.{SelectFieldSpark, SelectMetadataChangeSpark, SelectValuesObjectMeta}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.core.row.ValueMetaInterface
import org.pentaho.di.core.row.value.ValueMetaBase
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.selectvalues.{SelectValuesData, SelectValuesMeta}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
class SelectValuesSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
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
    var selectValuesMeta: SelectValuesMeta = null
//    var selectValuesData: SelectValuesData = null

    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
        selectValuesMeta = smi.asInstanceOf[SelectValuesMeta]
//        selectValuesData = sdi.asInstanceOf[SelectValuesData]
        val dataFrame: DataFrame = getData()
        try{
          if (null == dataFrame) {
              setOutputDone()
              return false
          }
          if(null != dataFrame){
              val select = !selectValuesMeta.getSelectFields.isEmpty
              val selectFieldSparks = new ArrayBuffer[SelectFieldSpark]()
              selectValuesMeta.getSelectFields().map(data=>{
                  selectFieldSparks+=new SelectFieldSpark(data.getName,data.getRename,data.getLength,data.getPrecision)
              })
              val deselect = !selectValuesMeta.getDeleteName.isEmpty
              val deleteFields = selectValuesMeta.getDeleteName
              val metadata = !selectValuesMeta.getMeta.isEmpty
              val selectMetadataChangeSparks = new ArrayBuffer[SelectMetadataChangeSpark]()
              selectValuesMeta.getMeta.map(data=>{
                  selectMetadataChangeSparks+=new SelectMetadataChangeSpark(data.getName,data.getRename,data.getType,data.getLength,data.getPrecision,data.getStorageType,data.getConversionMask,data.isDateFormatLenient,data.getDateFormatLocale,data.getDateFormatTimeZone,data.isLenientStringToNumber,data.getDecimalSymbol,data.getGroupingSymbol,data.getCurrencySymbol,data.getEncoding)
              })
             val selectValuesObjectMeta =  SelectValuesObjectMeta(select,selectFieldSparks.toArray,
                 deselect,deleteFields,metadata,selectMetadataChangeSparks.toArray
             )
             val dataFrameSelect = dataFrame.select(selectFieldSparks.map(f=>f.name).map(col):_*)
              val fdata = SelectValuesObject.checkData(selectValuesObjectMeta,dataFrameSelect
                  ,sparkSession,stepName,stepId)
              putData(null,fdata)
          }
        }catch {
          case e:Exception=>{
            putErrorData(e.getMessage,dataFrame)
          }
        }
        true
    }


}
