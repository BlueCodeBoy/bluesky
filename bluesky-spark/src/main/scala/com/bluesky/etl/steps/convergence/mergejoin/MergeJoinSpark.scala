package com.bluesky.etl.steps.convergence.mergejoin

import java.util.concurrent.TimeUnit

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql._
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.mergejoin.{MergeJoinDataForSpark, MergeJoinMetaForSpark}
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.apache.spark.sql.functions._
/**
  * Created by root on 18-11-1.
  */
class MergeJoinSpark (sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
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
  var mergeJoinMeta:MergeJoinMetaForSpark = null
  var mergeJoinData:MergeJoinDataForSpark = null
  var mergeJoinMetaObject:MergeJoinMetaObject = null
  /**
    *
    * @param sparkSession
    * @param smi
    * @param sdi
    * @return
    */
  override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
    mergeJoinMeta = smi.asInstanceOf[MergeJoinMetaForSpark]
    mergeJoinData = sdi.asInstanceOf[MergeJoinDataForSpark]
    val one = mergeJoinMeta.getStepIOMeta().getInfoStreams().get(0).getStepname
    val two = mergeJoinMeta.getStepIOMeta().getInfoStreams().get(1).getStepname
    if(null == mergeJoinMetaObject) {
      mergeJoinMetaObject = new MergeJoinMetaObject(one, two,
        mergeJoinMeta.getKeyFields1,
        mergeJoinMeta.getKeyFields2,
        mergeJoinMeta.getJoinType.toLowerCase
      )
    }
    val finalDataFrame: DataFrame = MergeJoinObject.processData(mergeJoinMetaObject)
    if (null == finalDataFrame) {
      TimeUnit.MILLISECONDS.sleep(500)
//      setOutputDone()
//      return false
    }
    if(null!=finalDataFrame){
      putData(null,finalDataFrame)
    }
    true
  }
  case class MergeJoinMetaObject( one:String,two:String,
                                          joinkey1:Array[String],
                                          joinkey2:Array[String],
                                          joinType:String
                                        ) extends BaseMeta with Serializable{
  }/**
    *
    */
  object MergeJoinObject {
    def processData(mergeJoinMetaObject:MergeJoinMetaObject):DataFrame = {
      var finalDataFrame:DataFrame = null
      val oneDataFrame = getDataByPrevStep(mergeJoinMetaObject.one)
      val twoDataFrame = getDataByPrevStep(mergeJoinMetaObject.two)
      var joinCondition:Column = null
      if(null !=oneDataFrame && null !=twoDataFrame){
        if(mergeJoinMetaObject.joinkey1.length==mergeJoinMetaObject.joinkey2.length){
          val onSchema = oneDataFrame.schema.fields.map(data=>{data.name})
          val twoSchema = twoDataFrame.schema.fields.map(data=>{data.name})
          for(x <- 0 until mergeJoinMetaObject.joinkey2.length){
            if(onSchema.contains(mergeJoinMetaObject.joinkey1.apply(x))&&
              twoSchema.contains(mergeJoinMetaObject.joinkey2.apply(x))){
              if(null == joinCondition){
                joinCondition = oneDataFrame(mergeJoinMetaObject.joinkey1.apply(x)) ===
                  twoDataFrame(mergeJoinMetaObject.joinkey2.apply(x))
              } else {
                joinCondition = joinCondition && oneDataFrame(mergeJoinMetaObject.joinkey1.apply(x)) ===
                    twoDataFrame(mergeJoinMetaObject.joinkey2.apply(x))
              }
            }
          }
        } else{
          logInfo("size of key1 not eq key2")
          throw new RuntimeException("size of (key1 !=key2) ")
        }
      }
      if(null !=joinCondition){
        val intersect = oneDataFrame.schema.fieldNames intersect twoDataFrame.schema.fieldNames
        finalDataFrame = oneDataFrame.join(twoDataFrame,joinCondition,mergeJoinMetaObject.joinType)
        if(!intersect.isEmpty){
          for(col<-intersect){
            finalDataFrame = finalDataFrame.drop(twoDataFrame(col))
          }
        }
      }
      finalDataFrame
    }
  }
}
