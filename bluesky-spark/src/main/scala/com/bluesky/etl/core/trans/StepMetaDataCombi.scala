package com.bluesky.etl.core.trans

import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}

/**
  * Created by root on 18-8-14.
  */
class StepMetaDataCombi {
  var stepMeta: StepMeta = null
  var stepname: String = null
  var copy = 0

  var step: SparkStepInterface = null
  var meta: StepMetaInterface = null
  var data: StepDataInterface = null

  override def toString: String = step.toString
}
