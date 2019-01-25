package com.bluesky.etl.core.trans

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Calendar, Date, Map}

import com.bluesky.etl.core.enums.{CommonFieldType, CommonStepId, ConstanceType}
import com.bluesky.etl.core.queue.{BlockingRowSet, RowSet}
import com.bluesky.etl.core.trans
import com.bluesky.etl.utils.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.pentaho.di.core.row.RowMetaInterface
import org.pentaho.di.trans.step.BaseStepData.StepExecutionStatus
import org.pentaho.di.trans.step.{StepDataInterface, StepListener, StepMeta, StepMetaInterface}
import org.pentaho.di.trans.steps.mapping.Mapping
import org.pentaho.di.trans.{Trans, TransMeta}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}
/**
  * Created by root on 18-8-15.
  */
abstract class BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                        trans: Trans, stepDataInterfaces: StepDataInterface, baseMeta:BaseMeta) extends SparkStepInterface  with Logging{
  val restoreObject:Object = new Object()
  protected final val stepName: String = stepMeta.getName
  protected final val stepId: String = stepMeta.getStepID
  protected val stepCopy: Int = copyNr
  protected val stepDataInterface: StepDataInterface = stepDataInterfaces
  private var inputRowMeta: RowMetaInterface = null
  private var inputRowSets: ArrayBuffer[RowSet] = null
  private var restoreDataFrame: ArrayBuffer[Dataset[Row]] = new ArrayBuffer[Dataset[Row]]()
  private var outputRowSets: ArrayBuffer[RowSet] = null
  private var nextSteps: Array[StepMeta] = null
  private var prevSteps: Array[StepMeta] = null
  private var currentInputRowSetNr = 0
  private var currentOutputRowSetNr = 0
  private var errorRowSet: RowSet = null
  private var errors = 0L
  private var canLoopAddData = false
  private var checkTransRunning = false
  var rowWithBroadCast: Dataset[Row] = null
  var mapRowWithBroadCast: mutable.Map[String,Dataset[Row]] = new mutable.HashMap[String,Dataset[Row]]()
  /** the rowset for the error rows */
  private val running: AtomicBoolean = new AtomicBoolean(false)
  private val stopped: AtomicBoolean = new AtomicBoolean(false)
  private val paused: AtomicBoolean = new AtomicBoolean(false)
  private var start_time: Date = null
  private var stop_time: Date = null
  var bsdata:(StructType,Broadcast[List[Row]]) = null
  private val stepListeners: ArrayBuffer[StepListener] = new ArrayBuffer[StepListener]()
//  import sparkSession.implicits._
//  @volatile val emptyDataFrame = Seq.empty[(String, Int)].toDF("k", "v")
  //分发
  dispatch()




  /**
    * Find output row set.
    *
    * @param targetStep
    * the target step
    * @return the row set
    * the kettle step exception
    */
  def findOutputRowSet(targetStep: String): RowSet = { // Check to see that "targetStep" only runs in a single copy
    // Otherwise you'll see problems during execution.
    //
    val targetStepMeta = transMeta.findStep(targetStep)
    if (targetStepMeta == null) throw new RuntimeException("BaseStep.Exception.TargetStepToWriteToDoesntExist"+targetStep)

    if (targetStepMeta.getCopies > 1) throw new RuntimeException("BaseStep.Exception.TargetStepToWriteToCantRunInMultipleCopies"+targetStep+Integer.toString(targetStepMeta.getCopies))
    findOutputRowSet(getStepname, getCopy, targetStep, 0)
  }


  /**
    * Find an output rowset in a running transformation. It will also look at the "to" step to see if this is a mapping.
    * If it is, it will find the appropriate rowset in that transformation.
    *
    * @param from
    * @param fromcopy
    * @param to
    * @param tocopy
    * @return The rowset or null if none is found.
    */
  def findOutputRowSet(from: String, fromcopy: Int, to: String, tocopy: Int): RowSet = {
    import scala.collection.JavaConversions._
    for (rs <- outputRowSets) {
      if (rs.getOriginStepName.equalsIgnoreCase(from) && rs.getDestinationStepName.equalsIgnoreCase(to) && rs.getOriginStepCopy == fromcopy && rs.getDestinationStepCopy == tocopy) return rs
    }
    // See if the rowset is part of the input of a mapping target step...
    //
    // Lookup step "To"
    //
    val mappingStep = transMeta.findStep(to)
    // See if it's a mapping
    if (mappingStep != null && mappingStep.isMapping) { // In this case we can cast the step thread to a Mapping...
      val baseSteps = trans.findBaseSteps(to)
      if (baseSteps.size == 1) {
        val mapping = baseSteps.get(0).asInstanceOf[Mapping]
        // Find the appropriate rowset in the mapping...
        // The rowset in question has been passed over to a Mapping Input step inside the Mapping transformation.
        val inputs = mapping.getMappingTrans.findMappingInput
        for (input <- inputs) {
          import scala.collection.JavaConversions._
          for (rs <- input.getInputRowSets) { // The source step is what counts in this case...
            if (rs.getOriginStepName.equalsIgnoreCase(from)) {
                return null
//              return rs
            }
          }
        }
      }
    }
    // Still nothing found!
    null
  }


  def dispatch(): Unit = {
    val stepMeta = transMeta.findStep(stepName)
    // How many next steps are there? 0, 1 or more??
    // How many steps do we send output to?
    val previousSteps: util.List[StepMeta] = transMeta.findPreviousSteps(stepMeta, true)
    val succeedingSteps: util.List[StepMeta] = transMeta.findNextSteps(stepMeta)

    val nrInput: Int = previousSteps.size
    val nrOutput: Int = succeedingSteps.size

    inputRowSets = new ArrayBuffer[RowSet]
    outputRowSets = new ArrayBuffer[RowSet]
    errorRowSet =new BlockingRowSet(20)
    prevSteps = new Array[StepMeta](nrInput)
    nextSteps = new Array[StepMeta](nrOutput)
    currentInputRowSetNr = 0
    // populate input rowsets.
    var rowSet: RowSet = null
    var i = 0
    while ( {
      i < previousSteps.size
    }) {
      prevSteps(i) = previousSteps.get(i)
      // Looking at the previous step, you can have either 1 rowset to look at or more then one.
      val prevCopies = prevSteps(i).getCopies
      val nextCopies = stepMeta.getCopies
      var nrCopies = 0
      var dispatchType = 0
      var repartitioning = false
      if (prevSteps(i).isPartitioned) repartitioning = !(prevSteps(i).getStepPartitioningMeta == stepMeta.getStepPartitioningMeta)
      else repartitioning = stepMeta.isPartitioned
      if (prevCopies == 1 && nextCopies == 1) { // normal hop
        dispatchType = org.pentaho.di.trans.Trans.TYPE_DISP_1_1
        nrCopies = 1
      }
      else if (prevCopies == 1 && nextCopies > 1) { // one to many hop
        dispatchType = org.pentaho.di.trans.Trans.TYPE_DISP_1_N
        nrCopies = 1
      }
      else if (prevCopies > 1 && nextCopies == 1) { // from many to one hop
        dispatchType = org.pentaho.di.trans.Trans.TYPE_DISP_N_1
        nrCopies = prevCopies
      }
      else if (prevCopies == nextCopies && !repartitioning) { // this may be many-to-many or swim-lanes hop
        dispatchType = org.pentaho.di.trans.Trans.TYPE_DISP_N_N
        nrCopies = 1
      }
      else { // > 1!
        dispatchType = org.pentaho.di.trans.Trans.TYPE_DISP_N_M
        nrCopies = prevCopies
      }
      var c = 0
      while ( {
        c < nrCopies
      }) {
        rowSet = null
        dispatchType match {
          case org.pentaho.di.trans.Trans.TYPE_DISP_1_1 =>
            rowSet = trans.findRowSet(prevSteps(i).getName, 0, stepName, 0)
          case org.pentaho.di.trans.Trans.TYPE_DISP_1_N =>
            rowSet = trans.findRowSet(prevSteps(i).getName, 0, stepName, getCopy)
          case org.pentaho.di.trans.Trans.TYPE_DISP_N_1 =>
            rowSet = trans.findRowSet(prevSteps(i).getName, c, stepName, 0)
          case org.pentaho.di.trans.Trans.TYPE_DISP_N_N =>
            rowSet = trans.findRowSet(prevSteps(i).getName, getCopy, stepName, getCopy)
          case org.pentaho.di.trans.Trans.TYPE_DISP_N_M =>
            rowSet = trans.findRowSet(prevSteps(i).getName, c, stepName, getCopy)
          case _ =>
        }
        if (rowSet != null) {
          inputRowSets.append(rowSet)
        }
        else if (!prevSteps(i).isMapping && !stepMeta.isMapping) {
          setErrors(1)
          stopAll()
          return
        }

        {
          c += 1
        }
      }

      {
        i += 1
      }
    }


    // And now the output part!
    var j = 0
    while ( {
      j < nrOutput
    }) {
      nextSteps(j) = succeedingSteps.get(j)
      val prevCopies = stepMeta.getCopies
      val nextCopies = nextSteps(j).getCopies
      var nrCopies = 0
      var dispatchType = 0
      var repartitioning = false
      if (stepMeta.isPartitioned) repartitioning = !(stepMeta.getStepPartitioningMeta == nextSteps(i).getStepPartitioningMeta)
      else repartitioning = nextSteps(j).isPartitioned
      if (prevCopies == 1 && nextCopies == 1) {
        dispatchType = Trans.TYPE_DISP_1_1
        nrCopies = 1
      }
      else if (prevCopies == 1 && nextCopies > 1) {
        dispatchType = Trans.TYPE_DISP_1_N
        nrCopies = nextCopies
      }
      else if (prevCopies > 1 && nextCopies == 1) {
        dispatchType = Trans.TYPE_DISP_N_1
        nrCopies = 1
      }
      else if (prevCopies == nextCopies && !repartitioning) {
        dispatchType = Trans.TYPE_DISP_N_N
        nrCopies = 1
      }
      else { // > 1!
        dispatchType = Trans.TYPE_DISP_N_M
        nrCopies = nextCopies
      }
      var c = 0
      while ( {
        c < nrCopies
      }) {
        rowSet = null
        dispatchType match {
          case Trans.TYPE_DISP_1_1 =>
            rowSet = trans.findRowSet(stepName, 0, nextSteps(j).getName, 0)
          case Trans.TYPE_DISP_1_N =>
            rowSet = trans.findRowSet(stepName, 0, nextSteps(j).getName, c)
          case Trans.TYPE_DISP_N_1 =>
            rowSet = trans.findRowSet(stepName, getCopy, nextSteps(j).getName, 0)
          case Trans.TYPE_DISP_N_N =>
            rowSet = trans.findRowSet(stepName, getCopy, nextSteps(j).getName, getCopy)
          case Trans.TYPE_DISP_N_M =>
            rowSet = trans.findRowSet(stepName, getCopy, nextSteps(j).getName, c)
          case _ =>
        }
        if (rowSet != null) {
          outputRowSets.append(rowSet)
        }
        else if (!stepMeta.isMapping && !nextSteps(j).isMapping) {
          setErrors(1)
          stopAll()
          return
        }

        {
          c += 1
        }
      }

      {
        j += 1
      }
    }

    //    val a = "1"
  }



  /**
    * putRowTo is used to put a row in a certain specific RowSet.
    *
    * @param rowMeta
    * The row meta-data to put to the destination RowSet.
    * @param row
    * the data to put in the RowSet
    * @param rowSet
    * the RoWset to put the row into.
    * In case something unexpected goes wrong
    */
  def putDataTo(rowMeta: RowMetaInterface, row: Dataset[Row], rowSet: RowSet): Unit = { // Are we pausing the step? If so, stall forever...
    checkIsRestoreDataFrame(row)
    breakable {
      while ( {
        !rowSet.putRow(rowMeta, row)
      }) if (isStopped){
          break()
      }
    }
  }


  /**
    *
    * @param data
    */
  def checkIsRestoreDataFrame(data: Dataset[Row]):Unit={
    var flag:Int = 0
    var length = restoreDataFrame.length
    for(index<- 0 until length){
      if(restoreDataFrame.apply(index).equals(data)){
        flag = index
        restoreObject.synchronized{
          restoreObject.notifyAll()
        }
      }
    }
    if(flag>0){
      restoreDataFrame.remove(flag)
    }

  }
  /**
    * put error data to rowset
    * @param data
    */
  def putErrorData(msg:String, data: Dataset[Row]): Unit = {
    logInfo(s"start put error Data-${stepName}--${stepId}")
    checkIsRestoreDataFrame(data)
    if(trans.getisStreaming()) {
      logInfo(s"it is  streaming application-${stepName}--${stepId}")
        trans.realTimeErrorRowSet.putRowToError(msg,stepId,stepName, null, data)
    }else{
      logInfo(s"it is not streaming application-${stepName}--${stepId}")
    }
    logInfo(s"end put error Data-${stepName}--${stepId}")

  }

  /**
    *
    * @param rowMeta
    * @param data
    */
  def putBroadCastData(rowMeta: RowMetaInterface, data:(StructType,Broadcast[List[Row]]),update:Boolean =false ): Unit = {
    //
    if(null == data){
      return
    }
    //mu out put
    val outPutRowSetLenth = outputRowSets.size
    for(res<- 0 until outPutRowSetLenth){
      val rs = outputRowSets.apply(res)
      if(null == rs.getBroadCastRow || update){
        breakable {
          while ( {
            !rs.putBroadCastRow(rowMeta, data)
          }) if (isStopped) {
            break()
          }
        }
      }
    }
  }

  /**
    *
    * @param rowMeta
    * @param data
    */
  def putData(rowMeta: RowMetaInterface, data: Dataset[Row]): Unit = { // Are we pausing the step? If so, stall forever...

    //
    if(null == data){
      return
    }
    checkIsRestoreDataFrame(data)
    //put data to result rowSet
    val d = System.currentTimeMillis()
    if(!hasNextStep()){
//        if(trans.getisStreaming()){
//          trans.realTimeErrorRowSet.putRowWithTime(d,null,data)
//        }else{

//          if(!data.rdd.isEmpty()) {
//            trans.resultRowSet.synchronizedPutRow(null, data)
//          }
//        }

    }
    val outPutRowSetLenth = outputRowSets.size
    for(res<- 0 until outPutRowSetLenth){
      val rs = outputRowSets.apply(res)
      breakable {
        while ( {
          !rs.putRow(rowMeta, data)
        }){

        }
      }
    }

  }



  /**
    * There is a null situation. so it is not block
    * @param prvStepName
    * @return
    */
  def getDataByPrevStep(prvStepName:String): Dataset[Row] = {
    waitUntilTransformationIsStarted()
    if (inputRowSets.isEmpty) return null
    var inputRowSet: RowSet = null
    var row: Dataset[Row] = null
    if(null!=row){
      return row
    }
    if(inputRowSets.size>1){
      for(inputRS<-inputRowSets){
        if(inputRS.getOriginStepName.equals(prvStepName)){
          inputRowSet = inputRS
        }
      }
      if(inputRowSet==null){
        throw new RuntimeException("OriginStepName Stepid not match")
      }
    }else{
      inputRowSet = currentInputStream
    }
    while ( {
      row == null && !isStopped
    }) {
      bsdata= inputRowSet.getBroadCastRow
      if(null!=bsdata){
        if(null != mapRowWithBroadCast.getOrElse(prvStepName,null)){
          row =  mapRowWithBroadCast.get(prvStepName).get
        }else{
          rowWithBroadCast =  sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(bsdata._2.value),bsdata._1)
          mapRowWithBroadCast.put(prvStepName,rowWithBroadCast)
          row =  rowWithBroadCast
        }

      }
      if(null  == row){
        row = inputRowSet.getRowWait(1, TimeUnit.MILLISECONDS)
        if (stopped.get) {
          stopAll()
          return null
        }
      }
      if (row == null) {
        if (inputRowSet.isDone) {
          inputRowSets.remove(currentInputRowSetNr)
          if (inputRowSets.isEmpty) {
            if(row!=null){
              return row
            }else{
              return null
            }
          }
        }
      }
    }
    if (inputRowMeta == null) inputRowMeta = inputRowSet.getRowMeta
    return row
  }

  /**
    * get broadcast data first in get data function
    * @return
    */
  def getBroadCastData(): Dataset[Row] = {
    if(null!=rowWithBroadCast){
      return rowWithBroadCast
    }
    waitUntilTransformationIsStarted()
    if (inputRowSets.isEmpty) return null
    var inputRowSet: RowSet = null
    var row: Dataset[Row] = null
    inputRowSet = currentInputStream
    val bsdata = inputRowSet.getBroadCastRow
    if(null == bsdata){
      return null
    }
    rowWithBroadCast =  sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(bsdata._2.value),bsdata._1)
    return rowWithBroadCast
  }

  /**
    *
    * @return
    */
  def getData(): Dataset[Row] = {
    waitUntilTransformationIsStarted()
    if (inputRowSets.isEmpty) return null
    var inputRowSet: RowSet = null
    var row: Dataset[Row] = null
    inputRowSet = currentInputStream
    while ( {
      row == null && !isStopped
    }) {
      row = getBroadCastData()
      if(null == row){
        row = inputRowSet.getRowBLock
        if (stopped.get || null == row) {
          stopAll()
          return null
        }
      }
      // Try once more...
      if (row == null) {
        if (inputRowSet.isDone) {
            inputRowSets.remove(currentInputRowSetNr)
            if (inputRowSets.isEmpty) {
              if(row!=null){
                return row
              }else{
                return null
              }
            }
        }
      }
    }
    if (inputRowMeta == null) inputRowMeta = inputRowSet.getRowMeta
    return row
  }



  protected def waitUntilTransformationIsStarted(): Unit = { // Have all threads started?
    // Are we running yet? If not, wait a bit until all threads have been
    // started.
    //
    if (this.checkTransRunning == false) {
      while ( {
        !trans.isRunning && !stopped.get
      }) try
        Thread.sleep(1)
      catch {
        case e: InterruptedException =>

        // Ignore sleep interruption exception
      }
      this.checkTransRunning = true
    }
  }

  /**
    *
    */
  def setOutputDone(): Unit = {
    //check streaming and wait
    if(trans.getisStreaming()){
      trans.streamObject.synchronized{
        trans.streamObject.wait()
      }
    }
    outputRowSets.synchronized {
      var i = 0
      while ( {
        i < outputRowSets.size
      }) {
        val rs = outputRowSets.apply(i)
        rs.setDone()
        i += 1
      }
      if (errorRowSet != null) errorRowSet.setDone()
    }

  }

  /**
    * Current input stream.
    *
    * @return the row set
    */
  private def currentInputStream = inputRowSets.apply(currentInputRowSetNr)

  /**
    * Mark the start time of the step.
    *
    */
  override def markStart(): Unit = {
    val cal = Calendar.getInstance
    start_time = cal.getTime
  }

  /**
    * Mark the end time of the step.
    *
    */
  override def markStop(): Unit = {
    val cal = Calendar.getInstance
    stop_time = cal.getTime

    // Here we are completely done with the transformation.
    // Call all the attached listeners and notify the outside world that the step has finished.
    //
    stepListeners.synchronized {
      for (stepListener <- stepListeners) {
        //        stepListener.stepFinished(trans, stepMeta, this.asInstanceOf[org.pentaho.di.trans.step.StepInterface])
      }
    }


    // We're finally completely done with this step.

    setRunning(false)
    if(trans.getisStreaming()){
      trans.stopAll()
    }
  }

  /**
    * Flag the step as running or not
    *
    * @param running
    * the running flag to set
    */
  override def setRunning(running: Boolean): Unit = {
    this.running.set(running)
  }

  /**
    * Stop running operations...
    *
    * @param stepMetaInterface
    * The metadata that might be needed by the step to stop running.
    * @param stepDataInterface
    * The interface to the step data containing the connections, resultsets, open files, etc.
    *
    */
  override def stopRunning(stepMetaInterface: StepMetaInterface, stepDataInterface: StepDataInterface): Unit = {
  }

  /**
    * @return true if the step is running after having been initialized
    */
  override def isRunning: Boolean = running.get()

  /**
    * @return True if the step is paused
    */
  override def isPaused: Boolean = paused.get()

  /**
    * Pause a running step
    */
  override def pauseRunning(): Unit = {
    setPaused(true)
  }

  /**
    * Resume a running step
    */
  override def resumeRunning(): Unit = {
    setPaused(false)
  }

  def setPaused(paused: Boolean): Unit = {
    this.paused.set(paused)
  }

  /**
    * Dispose of this step: close files, empty logs, etc.
    *
    * @param sii
    * The metadata to work with
    * @param sdi
    * The data to dispose of
    */
  override def dispose(sii: StepMetaInterface, sdi: StepDataInterface): Unit = {
    sdi.setStatus(StepExecutionStatus.STATUS_DISPOSED)
  }

  /**
    * put datato input
    *
    * @param data
    */
  def putDataToInputRowSet(data: Dataset[Row]):Unit={
    for(rowset<-inputRowSets){
      rowset.putRow(null,data)
    }
    restoreDataFrame += data
  }
  def getcanLoopAddData=canLoopAddData
  override def getStepname: String = stepName

  /**
    *
    * @return
    */
  def hasNextStep():Boolean={
    if(null==nextSteps || nextSteps.isEmpty){
      false
    }else{
      true
    }
  }
  /**
    * @return Returns the inputRowSets.
    */
  def getInputRowSets: ArrayBuffer[RowSet] = inputRowSets

  /**
    *
    * @param sparkSession
    * @param smi
    * @param sdi
    * @return
    */
  override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean

  def getCopy: Int = stepCopy

  override def setErrors(e: Long): Unit = {
    errors = e
  }

  /**
    * @return True if the step is marked as stopped. Execution should stop immediate.
    */
  override def isStopped: Boolean = stopped.get()

  /**
    * @param stopped
    * true if the step needs to be stopped
    */
  override def setStopped(stopped: Boolean): Unit = {
    this.stopped.set(stopped)
  }

  /**
    * Flags all rowsets as stopped/completed/finished.
    */
  override def stopAll(): Unit = {
    val outPutRowSetLenth = outputRowSets.size
    for(res<- 0 until outPutRowSetLenth){
      val rs = outputRowSets.apply(res)
      breakable {
        while ( {
          !rs.putRow(null, null)
        }){

        }
      }
    }
    stopped.set(true)
    //history thread
    trans.realTimeHistoryRowSet.putRow(null,null)
    //save error row thread
    trans.realTimeErrorRowSet.putRowToError(null,null,null,null, null)
    //statistic row
    val d   = System.currentTimeMillis()
    trans.realTimeErrorRowSet.putRowWithTime(d,d,null,null)
    trans.stopAll()
  }

}
