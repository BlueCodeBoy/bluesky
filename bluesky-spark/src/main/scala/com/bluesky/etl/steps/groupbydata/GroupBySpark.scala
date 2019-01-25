package com.bluesky.etl.steps.groupbydata

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import com.bluesky.etl.steps.groupbydata.GroupByObject.GroupByObjectMeta
import org.apache.spark.sql._
import org.pentaho.di.trans.groupby.{MemoryGroupByDataForSpark, MemoryGroupByMetaForSpark}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
class GroupBySpark(sparkSession: SparkSession, transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                   trans: Trans, stepDataInterface: StepDataInterface, baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
    trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface {
    var groupByMeta: MemoryGroupByMetaForSpark = null
    var groupByData: MemoryGroupByDataForSpark = null
    var groupByObjectMeta:GroupByObjectMeta = null

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
        groupByMeta = smi.asInstanceOf[MemoryGroupByMetaForSpark]
        groupByData = sdi.asInstanceOf[MemoryGroupByDataForSpark]
        val dataFrame: DataFrame = getData()
        try{
          if (null == dataFrame) {
            setOutputDone()
            return false
          }
          if(null != dataFrame){
            if(null == groupByObjectMeta){
              groupByObjectMeta = new GroupByObjectMeta(groupByMeta.getAggregateField.toList,
                groupByMeta.getAggregateType.map(index=>MemoryGroupByMetaForSpark.typeGroupCode.apply(index)).toList,
                groupByMeta.getSubjectField.toList,
                groupByMeta.getValueField.toList,
                groupByMeta.getGroupField.toList,
                MemoryGroupByMetaForSpark.typeGroupCode.toList)
            }
            val fields = dataFrame.schema.fieldNames
            val boolean =  groupByObjectMeta.subjectField.map(key=>if(fields.indexOf(key) == -1) false else  true).filter(b=>b==false).isEmpty
            if(!boolean){
              throw new RuntimeException("field not in dataframe")
            }
            val df = GroupByObject.groupData(groupByObjectMeta,dataFrame,sparkSession)
            putData(null,df)
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
