package com.bluesky.etl.core.trans

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.pentaho.di.trans.step.{StepDataInterface, StepMetaInterface}

/**
  * Created by root on 18-8-14.
  */
trait SparkStepInterface extends Serializable{
  /**
    *
    * @param sparkSession
    * @param smi
    * @param sdi
    * @return
    */
  def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean

  /**
    *
    * @param data
    */
  def restoreData( data: Dataset[Row]): Unit

  /**
    *
    * @return
    */
  def getStepname: String
  /**
    * Mark the start time of the step.
    *
    */
  def markStart(): Unit

  /**
    * Mark the end time of the step.
    *
    */
  def markStop(): Unit

  /**
    * Stop running operations...
    *
    * @param stepMetaInterface
    * The metadata that might be needed by the step to stop running.
    * @param stepDataInterface
    * The interface to the step data containing the connections, resultsets, open files, etc.
    *
    */
  def stopRunning(stepMetaInterface: StepMetaInterface, stepDataInterface: StepDataInterface): Unit

  /**
    * @return true if the step is running after having been initialized
    */
  def isRunning: Boolean

  /**
    * Flag the step as running or not
    *
    * @param running
    * the running flag to set
    */
  def setRunning(running: Boolean): Unit

  /**
    * @return True if the step is marked as stopped. Execution should stop immediate.
    */
  def isStopped: Boolean

  /**
    * @param stopped
    * true if the step needs to be stopped
    */
  def setStopped(stopped: Boolean): Unit

  /**
    * @return True if the step is paused
    */
  def isPaused: Boolean

  /**
    * Flags all rowsets as stopped/completed/finished.
    */
  def stopAll(): Unit

  /**
    * Pause a running step
    */
  def pauseRunning(): Unit

  /**
    * Resume a running step
    */
  def resumeRunning(): Unit


  /**
    * Sets the number of errors
    *
    * @param errors
    * the number of errors to set
    */
  def setErrors(errors: Long): Unit

  /**
    * Dispose of this step: close files, empty logs, etc.
    *
    * @param sii
    * The metadata to work with
    * @param sdi
    * The data to dispose of
    */
  def dispose(sii: StepMetaInterface, sdi: StepDataInterface): Unit


}
