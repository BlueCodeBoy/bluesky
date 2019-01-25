package com.bluesky.etl.steps.switchcase

import com.bluesky.etl.core._
import com.bluesky.etl.core.queue.RowSet
import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.switchcase.{SwitchCaseData, SwitchCaseMeta, SwitchCaseTarget}

class SwitchCaseObject(fieldnames:String,targetss:java.util.List[SwitchCaseTarget])  extends BaseMeta with Serializable{
  val fieldname = fieldnames
  val targets = targetss
}
/**
  * Created by root on 18-8-14.
  */
class SwitchCaseSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                      trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
  trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface {
  /**
    *
    * @param data
    */
  override def restoreData(data: Dataset[Row]): Unit = {
    putDataToInputRowSet(data)
     restoreObject.synchronized{      restoreObject.wait()    }
  }
  private var switchCaseMeta: SwitchCaseMeta = null
  private var switchCaseData: SwitchCaseData = null

  override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
    switchCaseMeta = smi.asInstanceOf[SwitchCaseMeta]
    switchCaseData = sdi.asInstanceOf[SwitchCaseData]
    val switchCaseObject = new SwitchCaseObject(switchCaseMeta.getFieldname,switchCaseMeta.getCaseTargets())
    val dataframe: DataFrame = getData()
    if(dataframe!=null){
      //next steps size
      var nrTargets = switchCaseObject.targets.size
      var target:SwitchCaseTarget = null
      var n = 0
      while ( {
        n < nrTargets
      }) {
        target = switchCaseObject.targets.get(n)
        if (target.caseTargetStep == null) {
          throw new RuntimeException("SwitchCase.Log.NoTargetStepSpecifiedForValue"+target.caseValue)
        }
        val rowSet:RowSet = findOutputRowSet(target.caseTargetStep.getName)
        putDataTo(null,
          dataframe.filter(dataframe(switchCaseObject.fieldname) === target.caseValue),
          rowSet)
        n+=1
      }
    } else{
      setOutputDone()
      return false
    }
    true
  }
}
