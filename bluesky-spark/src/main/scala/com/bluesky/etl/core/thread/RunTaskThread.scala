package com.bluesky.etl.core.thread

import com.bluesky.etl.core.trans.SparkStepInterface
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.SparkSession
import org.pentaho.di.i18n.BaseMessages
import org.pentaho.di.trans.step.{StepDataInterface, StepMetaInterface}

import scala.util.control.Breaks._

/**
  * Created by root on 18-8-15.
  */
class RunTaskThread(step: SparkStepInterface,
                    meta: StepMetaInterface,
                    data: StepDataInterface,
                    sparkSession: SparkSession) extends Runnable with Logging {

  override def run(): Unit = {
    Thread.currentThread().setName(step.getStepname)
    try {
      step.setRunning(true)
      breakable {
        while ( {
          step.processData(sparkSession, meta, data)
        }) {
          if (step.isStopped) {
            break()
          }
        }
      }

    } catch {
      case t: Exception =>
        logError(s"the thread (${step.getStepname}) is dead . error message is ${t.getMessage}",t)
        t.printStackTrace()
        try { // check for OOME
          if (t.isInstanceOf[OutOfMemoryError]) { // Handle this different with as less overhead as possible to get an error message in the log.
            logError("UnexpectedError: ", t)
          }
          else {
            t.printStackTrace()
            logError(BaseMessages.getString("System.Log.UnexpectedError"), t)
          }
        } catch {
          case e: OutOfMemoryError =>
            e.printStackTrace()
        } finally {
          step.setErrors(1)
          step.stopAll()
        }
    } finally {
      step.dispose(meta, data)
      ////      step.getLogChannel.snap(Metrics.METRIC_STEP_EXECUTION_STOP)
      //      try {
      ////        val li: Long = step.getLinesInput
      ////        val lo: Long = step.getLinesOutput
      ////        val lr: Long = step.getLinesRead
      ////        val lw: Long = step.getLinesWritten
      ////        val lu: Long = step.getLinesUpdated
      ////        val lj: Long = step.getLinesRejected
      ////        val e: Long = step.getErrors
      ////        if (li > 0 || lo > 0 || lr > 0 || lw > 0 || lu > 0 || lj > 0 || e > 0) log.logBasic(BaseMessages.getString(PKG, "BaseStep.Log.SummaryInfo", String.valueOf(li), String.valueOf(lo), String.valueOf(lr), String.valueOf(lw), String.valueOf(lu), String.valueOf(e + lj)))
      ////        else log.logDetailed(BaseMessages.getString(PKG, "BaseStep.Log.SummaryInfo", String.valueOf(li), String.valueOf(lo), String.valueOf(lr), String.valueOf(lw), String.valueOf(lu), String.valueOf(e + lj)))
      //      } catch {
      //        case t: Throwable =>
      //          //
      //          // it's likely an OOME, so we don't want to introduce overhead by using BaseMessages.getString(), see above
      //          //
      //          log.logError("UnexpectedError: " + Const.getStackTracker(t))
      step.markStop()
    }

  }
}
