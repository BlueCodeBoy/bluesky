package com.bluesky.etl.steps.filterrows

import java.util
import java.util.Arrays

import com.bluesky.etl.core.queue.RowSet
import org.apache.spark.sql.functions.negate
import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import com.bluesky.etl.steps.selectvalues.SelectValuesObject.{SelectFieldSpark, SelectMetadataChangeSpark, SelectValuesObjectMeta}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.pentaho.di.core.{Condition, Const}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.filterrows.{FilterRowsData, FilterRowsMeta}
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta

import scala.collection.mutable.ArrayBuffer

class FilterRowsSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                      trans: Trans, stepDataInterface: StepDataInterface, baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
    trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface {
    var filterRowsMeta: FilterRowsMeta = null
    var filterRowsData: FilterRowsData = null

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
        filterRowsMeta = smi.asInstanceOf[FilterRowsMeta]
        filterRowsData = sdi.asInstanceOf[FilterRowsData]
        val dataFrame: DataFrame = getData()
        try{
          if (null == dataFrame) {
            setOutputDone()
            return false
          }
          if(null != dataFrame){
            val targetStep = filterRowsMeta.getStepIOMeta.getTargetStreams
            var trueName:String = targetStep.get( 0 ).getStepname()
            var falseName:String = targetStep.get(1).getStepname

            val fr = new FilterRows()
            val column = fr.calConditionWithDataFrame(dataFrame, filterRowsMeta.getCondition)
            if(null!=column) {
              if (null != trueName) {
                val truedataFrame = dataFrame.filter(column)
                val rowSet: RowSet = findOutputRowSet(trueName)
                putDataTo(null, truedataFrame, rowSet)
              }
              if (null != falseName) {
                val falsedataFrame = dataFrame.filter(negate(column))
                val rowSet: RowSet = findOutputRowSet(falseName)
                putDataTo(null, falsedataFrame, rowSet)
              }
            }
          }
        }catch {
          case e:Exception=>{
            e.printStackTrace()
            logError(e.getMessage,e)
            putErrorData(e.getMessage,dataFrame)
          }
        }
        true

  }


}
