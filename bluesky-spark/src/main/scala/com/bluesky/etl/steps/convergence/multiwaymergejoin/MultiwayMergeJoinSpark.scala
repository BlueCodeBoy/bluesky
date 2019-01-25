package com.bluesky.etl.steps.convergence.multiwaymergejoin

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql._
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.multimerge.{MultiMergeJoinData, MultiMergeJoinMeta}

import scala.collection.JavaConverters._
/**
  *
  * @param transMeta
  * @param stepMeta
  * @param copyNr
  * @param trans
  * @param stepDataInterface
  * @param baseMeta
  */
class MultiwayMergeJoinSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
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
    var multiMergeJoinMeta:MultiMergeJoinMeta = null
    var multiMergeJoinData:MultiMergeJoinData = null

    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
        multiMergeJoinMeta = smi.asInstanceOf[MultiMergeJoinMeta]
        multiMergeJoinData = sdi.asInstanceOf[MultiMergeJoinData]
        val multiwayMergeJoinMetaObject = new MultiwayMergeJoinMetaObject(
        multiMergeJoinMeta.getInputSteps,
            multiMergeJoinMeta.getKeyFields,
            multiMergeJoinMeta.getJoinType)
        val finalDataFrame: DataFrame = MultiwayMergeJoinObject.processData(multiwayMergeJoinMetaObject)
        if (null == finalDataFrame) {
//            setOutputDone()
            return false
        }
        if(null!=finalDataFrame){
            putData(null,finalDataFrame)
        }
        true
    }
   case class MultiwayMergeJoinMetaObject(
                                       nameSeq:Array[String],
                                       indexedSeq:Array[String],
                                       joinType:String
                                     ) extends BaseMeta with Serializable{
    }
    /**
      *
      */
    object MultiwayMergeJoinObject {
        def processData(multiwayMergeJoinMetaObject:MultiwayMergeJoinMetaObject):DataFrame = {
            var finalDataFrame: DataFrame = null
            var prekey:String = null
            var currentKey:String = null
            var currentCondition:Array[String] = null
            var preCondition:Array[String] = null
            var joinCondition:Column = null
            for(index<-multiwayMergeJoinMetaObject.nameSeq.indices){
                if(index!=0){
                    prekey = multiwayMergeJoinMetaObject.indexedSeq.apply(index-1)
                }else{
                    prekey = null
                }
                currentKey = multiwayMergeJoinMetaObject.indexedSeq.apply(index)
                val tmpDataFrame = getDataByPrevStep(multiwayMergeJoinMetaObject.nameSeq.apply(index))
                if(null ==finalDataFrame){
                    finalDataFrame = tmpDataFrame
                }else{
                    currentCondition = currentKey.split(",")
                    preCondition = prekey.split(",")

                    for(c<-currentCondition){
                        for(d<-preCondition){
                            if(null == joinCondition){
                                joinCondition = tmpDataFrame(c) === finalDataFrame(d)
                            } else {
                                joinCondition = joinCondition &&tmpDataFrame(c) === finalDataFrame(d)
                            }
                        }
                    }
                    finalDataFrame = finalDataFrame.join(tmpDataFrame,joinCondition,multiwayMergeJoinMetaObject.joinType)
                }
            }
            finalDataFrame
        }
    }
}
