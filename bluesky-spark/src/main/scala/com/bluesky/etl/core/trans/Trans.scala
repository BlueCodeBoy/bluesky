package com.bluesky.etl.core.trans

import java.sql.Timestamp
import java.util
import java.util.{Date, Map}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bluesky.etl.core.entity.RuleDbInfoVo
import com.bluesky.etl.core.enums.{CommonFieldType, CommonStepId, ConstanceType}
import com.bluesky.etl.core.queue.{BlockingRowSet, RowSet}
import com.bluesky.etl.core.thread.{HistoryRowThread, _}
import com.bluesky.etl.steps.convergence.mergejoin.MergeJoinSpark
import com.bluesky.etl.steps.input.TableInput.TableInputSpark
import com.bluesky.etl.steps.input.kafkaconsumer.KafkaConsumerSpark
import com.bluesky.etl.steps.convergence.multiwaymergejoin.MultiwayMergeJoinSpark
import com.bluesky.etl.steps.output.deleterow.{DeleteRowSpark, DeleteSpark}
import com.bluesky.etl.steps.filterrows.FilterRowsSpark
import com.bluesky.etl.steps.groupbydata.GroupBySpark
import com.bluesky.etl.steps.output.kafkaproducer.KafkaProducerSpark
import com.bluesky.etl.steps.output.tableoutput.TableOutputSpark
import com.bluesky.etl.steps.selectvalues.SelectValuesSpark
import com.bluesky.etl.steps.switchcase.SwitchCaseSpark
import com.bluesky.etl.steps.output.update.UpdateSpark
import com.bluesky.etl.utils.{HashUtils, HttpUtil, Logging, ThreadUtils}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.pentaho.di.core.Const
import org.pentaho.di.core.exception.KettleException
import org.pentaho.di.trans.TransMeta.TransformationType
import org.pentaho.di.trans.step.BaseStepData.StepExecutionStatus
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta}
import org.pentaho.di.trans.{TransMeta, TransStoppedListener}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try
import scala.util.control.Breaks._

/**
  * Created by root on 18-4-19.
  */
class Trans extends Logging {
  @volatile var attributesMap: util.Map[String, util.Map[String, String]] =  null
  private var errorTable:String = null
  private var successTable:String = null
  private var reportTable:String = null
  private var moveRightDataEnable:Boolean = false
  private var moveErrorDataEnable:Boolean = false
  private var rightDbMetadataUniqueCodeDest:String = null
  private var rightRuleDbinfoVo:RuleDbInfoVo = null
  private var errorRuleDbinfoVo:RuleDbInfoVo = null
  private var errorDbMetadataUniqueCodeDest:String = null
  private val batchNumber = System.currentTimeMillis()
  @volatile var resultRowSet: RowSet = new BlockingRowSet(20)
  @volatile var realTimeErrorRowSet:RowSet = new BlockingRowSet(20)
  @volatile var realTimeHistoryRowSet:RowSet = new BlockingRowSet(20)
  @volatile var pk: String = null
  @volatile private var isIncrement:Boolean = false
  @volatile private var canStoreHistore:Boolean = false
  @volatile private var isStreaming:Boolean = false
  @volatile var topicName:String = null
  @volatile var tranceName:String = null
  @volatile var tranceId:Int = 0
//  var externalThread = new ArrayBuffer[Runnable]()
  val streamObject:Object = new Object
  val obj:Object = new Object
  /** Constant indicating a dispatch type of 1-to-1. */
  val TYPE_DISP_1_1 = 1
  /** Constant indicating a dispatch type of 1-to-N. */
  val TYPE_DISP_1_N = 2
  /** Constant indicating a dispatch type of N-to-1. */
  val TYPE_DISP_N_1 = 3
  /** Constant indicating a dispatch type of N-to-N. */
  val TYPE_DISP_N_N = 4
  /** Constant indicating a dispatch type of N-to-M. */
  val TYPE_DISP_N_M = 5
  /** A list of all the row sets. */
  private var rowsets: ArrayBuffer[RowSet] = new ArrayBuffer[RowSet]()
  /** A list of all the steps. */
  private var steps: ArrayBuffer[StepMetaDataCombi] = new ArrayBuffer[StepMetaDataCombi]()
  /** Whether the transformation is running. */
  private var running = false
  /** Whether the transformation is paused. */
  private var paused: AtomicBoolean = new AtomicBoolean(false)

  /** Whether the transformation is stopped. */
  private var stopped: AtomicBoolean = new AtomicBoolean(false)
  private var transStoppedListeners: ArrayBuffer[TransStoppedListener] = new ArrayBuffer[TransStoppedListener]()

  def stopped_ = stopped.get()
  def queryAllStep():ArrayBuffer[StepMetaDataCombi]=steps
  /**
    *启动所以的线程
    */
  def fireAllThread(transMeta: TransMeta, sparkSession: SparkSession): Unit = {
    setRunning(true)
  val pool = ThreadUtils.newDaemonCachedThreadPool(
    "SparkEtl-newDaemonCachedThreadPool",
    steps.length+4,
    keepAliveSeconds = 2)
    try {
      for (stepMetaDataCombi <- steps) {
        pool.execute(new RunTaskThread(stepMetaDataCombi.step, stepMetaDataCombi.meta,
          stepMetaDataCombi.data, sparkSession))
      }
      //statistics
      val statisticRowThread:StatisticRowThread  = new StatisticRowThread(sparkSession,this)
      pool.execute(statisticRowThread)
//      externalThread+=statisticRowThread
      //saveErrorRow
      val saveErrorRowThread:SaveErrorRowThread  = new SaveErrorRowThread(sparkSession,this)
      pool.execute(saveErrorRowThread)
//      externalThread+=saveErrorRowThread
      //restoreErrorRow
      val restoreErrorRowThread:RestoreErrorRowThread  = new RestoreErrorRowThread(sparkSession,this,transMeta)
      pool.execute(restoreErrorRowThread)
//      externalThread+=restoreErrorRowThread
      //historyRow
      val historyRowThread:HistoryRowThread  = new HistoryRowThread(sparkSession,this)
      pool.execute(historyRowThread)
//      externalThread+=historyRowThread

      pool.shutdown()
    while(!pool.isTerminated){
      Thread.sleep(1000)
    }
    } finally {
      pool.shutdownNow()
      logInfo("task is finished!")
      setRunning(false)
    }

  }

  /**
    * 统计行同步效率
    * @param sparkSession
    */
  def statisticRowEfficiency(sparkSession: SparkSession):Unit={
    if(isStreaming){
      logInfo("starting statisitcing Row")

    }else{
      logInfo("this trance is not streaming application")
    }
  }
  /**
    * 搬移数据和统计
    * @param sparkSession
    */
  def startMoveDataAndStatistics(sparkSession: SparkSession):Unit={
    var unionDataFrame:DataFrame = null
    var dataFrame:DataFrame = null
    breakable {
      while ((dataFrame = resultRowSet.getRowImmediate)!=null){
        if(dataFrame==null){
          break()
        }else{
          if(unionDataFrame ==null){
            unionDataFrame = dataFrame
          }else{
            unionDataFrame = dataFrame.union(unionDataFrame)
          }
        }
      }
    }
    if(unionDataFrame!=null){
      if(!unionDataFrame.rdd.isEmpty()){
        logInfo("unionDataFrame first is not  null! and start move data and statsitc data")
        moveAction(sparkSession,unionDataFrame)
        statisticsData(sparkSession,unionDataFrame)
      }else{
        logInfo("unionDataFrame first is  null!")
      }
    }else{
      logInfo("unionDataFrame is null!(mean no data be moved  statstic)")
    }
  }
  /**
    *统计数据
    * @param sparkSession
    * @param dataFrame
    */
  def statisticsData(sparkSession: SparkSession,dataFrame: DataFrame):Unit={
    DataMoveStatisticsObject.statisticsData(sparkSession,dataFrame,isIncrement,reportTable,pk)
  }

  /**
    *搬移数据
    * @param sparkSession
    * @param dataFrame
    */
  def moveAction(sparkSession: SparkSession,dataFrame: DataFrame):Unit={
    DataMoveStatisticsObject.moveData(sparkSession,dataFrame,isIncrement,pk,moveErrorDataEnable,moveRightDataEnable,errorRuleDbinfoVo,errorTable,rightRuleDbinfoVo,successTable)
  }



  /**
    * Find the base steps for the step with the specified name.
    *
    * @param stepname
    * the step name
    * @return the list of base steps for the specified step
    */
  def findBaseSteps(stepname: String): ArrayBuffer[SparkStepInterface] = {
    val baseSteps = new ArrayBuffer [SparkStepInterface]
    if (steps == null) return baseSteps
    var i = 0
    while ( {
      i < steps.size
    }) {
      val sid = steps.apply(i)
      val stepInterface = sid.step
      if (stepInterface.getStepname.equalsIgnoreCase(stepname)) {
        baseSteps.append(stepInterface)
      }

        i += 1
    }
    baseSteps
  }
  def getCanStoreHistore():Boolean=canStoreHistore
  def getisStreaming():Boolean =isStreaming
  def getisIncrement():Boolean=isIncrement
  def getErrorTableName():String = errorTable
  def getSuccessTableName():String =successTable
  def getMoveRightDataEnable():Boolean =moveRightDataEnable
  def getMoveErrorDataEnable():Boolean=moveErrorDataEnable
  def getRightDbMetadataUniqueCodeDest():String=rightDbMetadataUniqueCodeDest
  def getErrorDbMetadataUniqueCodeDest():String=errorDbMetadataUniqueCodeDest
  def getbatchNumber():Long = batchNumber
  def getErrorRuleDbInfoVo():RuleDbInfoVo = errorRuleDbinfoVo
  def getRightRuleDbInfoVo():RuleDbInfoVo = rightRuleDbinfoVo

  /**
    *
    * @param baseUrl
    */
  private def getBaseMeta(baseUrl:String):Unit={
    try {
      if(moveRightDataEnable==true||moveErrorDataEnable==true){
        val content = HttpUtil.get(baseUrl+"public/rs/checkrule/getAllDBList.json?searchKey=")
        logInfo(s"the quality  datasource data${content}")
        val json = JSON.parseObject(content)
        val jsondata = json.get("data")
        if(null!=json){
          val jSONArray  = json.get("data").asInstanceOf[JSONObject].get("data").asInstanceOf[JSONArray].toArray
          for(json <- jSONArray){
            val obj = JSON.parseObject(json.asInstanceOf[JSONObject].toString, new RuleDbInfoVo().getClass)
            if(obj.getDbMetadataUniqueCode.equals(rightDbMetadataUniqueCodeDest)){
              rightRuleDbinfoVo = obj
            }
            if(obj.getDbMetadataUniqueCode.equals(errorDbMetadataUniqueCodeDest)){
              errorRuleDbinfoVo = obj
            }
          }
        }
      }
    } catch {
      case ioe:Exception =>
      logError("get dataerror"+ioe.getMessage)
      // handle this
    }

  }

  /**
    *
    * @param transMeta
    * @param baseUrl
    */
  def initParams(transMeta: TransMeta,baseUrl:String,jobID:String):Unit={
    val post = HashUtils.Md5hash(transMeta.getName)
    tranceName = jobID
    tranceId = transMeta.getObjectId.getId.toInt
    errorTable = s"error_$post"
    successTable = s"success_$post"
    reportTable = s"report_$post"
    if("Y".equals(transMeta.getAttribute("datamoveconfig","moveRightDataEnable"))){
      moveRightDataEnable = true
      rightDbMetadataUniqueCodeDest = transMeta.getAttribute("datamoveconfig","rightDbMetadataUniqueCodeDest")
    }
    if("Y".equals(transMeta.getAttribute("datamoveconfig","moveErrorDataEnable"))){
      moveErrorDataEnable = true
      errorDbMetadataUniqueCodeDest = transMeta.getAttribute("datamoveconfig","errorDbMetadataUniqueCodeDest")
    }
    if(null!=transMeta.getParameterDefault("streaming.store.history")&&transMeta.getParameterDefault("streaming.store.history").toString.equals("true")){
      canStoreHistore = true
    }
//    var url = baseUrl
//    if(!baseUrl.endsWith("/")){
//      url = baseUrl+"/"
//    }else{
//      url = baseUrl
//    }
//    getBaseMeta(url)
//    attributesMap =  transMeta.getAttributesMap

  }
  /**
    *
    * @param transMeta
    */
  def prepareExecution(transMeta: TransMeta,sparkSession: SparkSession): Unit = {
    val hopsteps = transMeta.getTransHopSteps(false)
    //init rowsets
    var i = 0
    var thisStep: StepMeta = null
    var nextSteps: java.util.List[StepMeta] = null
    var n = 0
    var nrTargets = 0
    var nextStep: StepMeta = null
    while ( {
      i < hopsteps.size
    }) {
      thisStep = hopsteps.get(i)
      breakable {
        if (thisStep.isMapping) {
          break()
        } //todo: continue is not supported// handled and allocated by the mapping step itself.
      }
      //find next steps
      nextSteps = transMeta.findNextSteps(thisStep)
      //next steps size
      nrTargets = nextSteps.size
      n = 0
      while ( {
        n < nrTargets
      }) { // What's the next step?
        nextStep = nextSteps.get(n)
        breakable {
          if (nextStep.isMapping) {
            break()
          }
        }
        // How many times do we start the source step?
        val thisCopies = thisStep.getCopies
        if (thisCopies < 0) { // This can only happen if a variable is used that didn't resolve to a positive
          // integer value
          //
          throw new KettleException("Trans.Log.StepCopiesNotCorrectlyDefined")
        }
        // How many times do we start the target step?
        val nextCopies = nextStep.getCopies
        // Are we re-partitioning?
        var repartitioning = false
        if (thisStep.isPartitioned) {
          repartitioning = !(thisStep.getStepPartitioningMeta == nextStep.getStepPartitioningMeta)
        } else {
          repartitioning = nextStep.isPartitioned
        }
        var nrCopies = 0
        var dispatchType = 0
        if (thisCopies == 1 && nextCopies == 1) {
          dispatchType = TYPE_DISP_1_1
          nrCopies = 1
        }
        else if (thisCopies == 1 && nextCopies > 1) {
          dispatchType = TYPE_DISP_1_N
          nrCopies = nextCopies
        } else if (thisCopies > 1 && nextCopies == 1) {
          dispatchType = TYPE_DISP_N_1
          nrCopies = thisCopies
        }
        else if (thisCopies == nextCopies && !repartitioning) {
          dispatchType = TYPE_DISP_N_N
          nrCopies = nextCopies
        }
        else { // > 1!
          dispatchType = TYPE_DISP_N_M
          nrCopies = nextCopies
        }
        // Allocate a rowset for each destination step}}}}
        // Allocate the rowsets
        if (dispatchType != TYPE_DISP_N_M) {
          var c = 0
          while ( {
            c < nrCopies
          }) {
            var rowSet: RowSet = null
            transMeta.getTransformationType match {
              case TransformationType.Normal =>
                // This is a temporary patch until the batching rowset has proven
                // to be working in all situations.
                // Currently there are stalling problems when dealing with small
                // amounts of rows.
                //                      val batchingRowSet = ValueMeta.convertStringToBoolean(System.getProperty(Const.KETTLE_BATCHING_ROWSET))
                //                      val batchingRowSet:Boolean = true;
                //                      if (batchingRowSet != null && batchingRowSet.booleanValue) {
                //                        rowSet = new BlockingBatchingRowSet(transMeta.getSizeRowset)
                //                      } else{
                rowSet = new BlockingRowSet(transMeta.getSizeRowset)
              //                      }
              case TransformationType.SerialSingleThreaded =>
              //                      rowSet = new SingleRowRowSet
              case TransformationType.SingleThreaded =>
              //                      rowSet = new QueueRowSet
              case _ =>
                throw new KettleException("Unhandled transformation type: " + transMeta.getTransformationType)
            }
            dispatchType match {
              case TYPE_DISP_1_1 =>
                rowSet.setThreadNameFromToCopy(thisStep.getName, 0, nextStep.getName, 0)
              case TYPE_DISP_1_N =>
                rowSet.setThreadNameFromToCopy(thisStep.getName, 0, nextStep.getName, c)
              case TYPE_DISP_N_1 =>
                rowSet.setThreadNameFromToCopy(thisStep.getName, c, nextStep.getName, 0)
              case TYPE_DISP_N_N =>
                rowSet.setThreadNameFromToCopy(thisStep.getName, c, nextStep.getName, c)
              case _ =>
            }
            rowsets.append(rowSet)
            c += 1
          }
        } else { // For each N source steps we have M target steps
          // From each input step we go to all output steps.
          // This allows maximum flexibility for re-partitioning,
          // distribution...
          var s = 0
          while ( {
            s < thisCopies
          }) {
            var t = 0
            while ( {
              t < nextCopies
            }) {
              val rowSet = new BlockingRowSet(transMeta.getSizeRowset)
              rowSet.setThreadNameFromToCopy(thisStep.getName, s, nextStep.getName, t)
              rowsets.append(rowSet)
              t += 1
            }
            s += 1
          }
        }
        n += 1
      }

      i += 1
    }
    //init StepMetaDataCombi
    var j: Int = 0
    var stepid: String = null
    var stepMeta: StepMeta = null
    var c: Int = 0
    var sparkStepInterface: SparkStepInterface = null
    while ( {
      j < hopsteps.size
    }) {
      stepMeta = hopsteps.get(j)
      stepid = stepMeta.getStepID
      // How many copies are launched of this step?
      val nrCopies: Int = stepMeta.getCopies
      // At least run once...
      c = 0
      while ( {
        c < nrCopies
      }) { // Make sure we haven't started it yet!
        if (!hasStepStarted(stepMeta.getName, c)) {
          val combi: StepMetaDataCombi = new StepMetaDataCombi
          combi.stepname = stepMeta.getName
          combi.copy = c
          // The meta-data
          combi.stepMeta = stepMeta
          combi.meta = stepMeta.getStepMetaInterface
          // Allocate the step data
          val data: StepDataInterface = combi.meta.getStepData
          combi.data = data
          // Allocate the step
          //          val step: StepInterface = combi.meta.getStep(stepMeta, data, c, transMeta, transMeta.tran)

          // Copy the variables of the transformation to the step...
          // don't share. Each copy of the step has its own variables.
          //
          //          step.initializeVariablesFrom(this)
          //          step.setUsingThreadPriorityManagment(transMeta.isUsingThreadPriorityManagment)
          // Pass the connected repository & metaStore to the steps runtime
          //          step.setRepository(repository)
          //          step.setMetaStore(metaStore)
          // If the step is partitioned, set the partitioning ID and some other
          // things as well...
          //          if (stepMeta.isPartitioned) {
          //            val partitionIDs: util.List[String] = stepMeta.getStepPartitioningMeta.getPartitionSchema.getPartitionIDs
          //            if (partitionIDs != null && partitionIDs.size > 0) {
          //              step.setPartitionID(partitionIDs.get(c)) // Pass the partition ID
          //
          //              // to the step
          //            }
          //          }
          // Save the step too
          //"Delete","WriteToLog","TextFileOutput","MultiwayMergeJoin","DBJoin","SFFieldEnum","SFFieldInterval","MergeJoin","HDFSDelete","AccessInput","HadoopExitPlugin","JsonInput","DDLCfg","TableOutput","MySQLBulkLoader","RandomCCNumberGenerator","XMLJoin","RegexEval","GetVariable","FieldFormatVerify","DBProc","MongoDbOutput","S3FileOutputPlugin","IfNull","ColumnExists","FileLocked","CheckSum","SocketWriter","SortedMerge","ExcelInput","ChangeFileEncoding","LoadFileInput","Janino","PGBulkLoader","getXMLData","KafkaInput","DataGrid","DetectLastRow","SymmetricCryptoTrans","AddXML","KafkaOutput","SortRows","TableInput","FieldsChangeSequence","RowsFromResult","JobExecutor","BlockUntilStepsFinish","AccessOutput","AutoDoc","HDFSInput","GetFileNames","ProcessFiles","StringOperations","SetVariable","BlobDBToFile","RandomValue","HBaseRowDecoder","HadoopFileInputPlugin","FieldSplitter","BlockingStep","MongoDbInput","PropertyInput","Append","ZipFile","SynchronizeAfterMerge","StringCut","MergeRows","StreamLookup","Dummy","Validator","PGPEncryptStream","SetValueField","GetFilesRowsCount","SystemInfo","Denormaliser","PGPDecryptStream","DynamicSQLRow","Update","ExcelOutput","Delay","UniqueRowsByHashSet","Formula","WebServiceAvailable","ExecProcess","HBaseOutput","NullIf","FileExists","SocketReader","GroupBy","FilesToResult","GetRepositoryNames","HDFSOutput","HTTP","Sequence","XSDValidator","SetValueConstant","FilterRows","ScriptValueMod","TypeExitExcelWriterStep","ConcatFields","BlobDBToDB","SwitchCase","FixedInput","DBLookup","TextFileInput","FieldMetadataAnnotation","CouchDbInput","Unique","CDCFromDBLog","UserDefinedJavaClass","PrioritizeStreams","OraBulkLoader","HBaseInput","StepsMetrics","ValueMapper","Constant","ExecSQLRow","JavaFilter","XMLOutput","RowsToResult","AvroInput","GetTableNames","ExecSQL","SFFieldRegular","HadoopFileOutputPlugin","Rest","Abort","SFFieldDefect","TransExecutor","PropertyOutput","JsonOutput","StepMetastructure","Calculator","SplitFieldToRows3","TableExists","DetectEmptyStream","Normaliser","SelectValues","XMLInputStream","FilesFromResult","Flattener","SSH","XSLT","InsertUpdate","MailInput","CloneRow","HTTPPOST","SQLFileOutput","GetSubFolders","HadoopEnterPlugin","ReplaceString","NumberRange","CreditCardValidator","WebServiceLookup","Mail","TableCompare","SecretKeyGenerator","MailValidator","RowGenerator","CreateSharedDimensions","TableSelector","DigitalPrecision","CountRecords","IDcardVerify","ForeignToPrimary","SFFieldComparision","SFFieldUnique"
          sparkStepInterface = caseSparkStepsById(sparkSession,stepid, transMeta, stepMeta, c, data)
          combi.step = sparkStepInterface
          // Pass logging level and metrics gathering down to the step level.
          //          // /
          //          if (combi.step.isInstanceOf[LoggingObjectInterface]) {
          //            val logChannel: LogChannelInterface = combi.step.getLogChannel
          //            logChannel.setLogLevel(logLevel)
          //            logChannel.setGatheringMetrics(log.isGatheringMetrics)
          //          }
          // Add to the bunch...
          steps.append(combi)
        }

        {
          c += 1
        }
      }

      {
        j += 1
      }
    }
    //no init thread
  }

  def hasStepStarted(sname: String, copy: Int): Boolean = { // log.logDetailed("DIS: Checking wether of not ["+sname+"]."+cnr+" has
    // started!");
    // log.logDetailed("DIS: hasStepStarted() looking in "+threads.size()+"
    // threads");
    var i = 0
    while ( {
      i < steps.size
    }) {
      val sid = steps.apply(i)
      val started = (sid.stepname != null && sid.stepname.equalsIgnoreCase(sname)) && sid.copy == copy
      if (started) return true

      {
        i += 1;
        i - 1
      }
    }
    false
  }

  /**
    * get spark step by stepid
    * @param sparkSession
    * @param stepid
    * @param transMeta
    * @param stepMeta
    * @param copyNr
    * @param sparkBaseStepData
    * @return
    */
  def caseSparkStepsById(sparkSession: SparkSession,stepid: String, transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int, sparkBaseStepData: StepDataInterface): SparkStepInterface = {
    stepid match {
      //分支
      case "SwitchCase" =>
        new SwitchCaseSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      //正则表达式
      case "KafkaConsumer" =>
        isStreaming = true
        new KafkaConsumerSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "KafkaProducer" =>
        new KafkaProducerSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "MultiwayMergeJoin" =>
        new MultiwayMergeJoinSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "SparkTableInput" =>
        new TableInputSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "SelectValues" =>
        new SelectValuesSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "Update" =>
        new UpdateSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "TableOutput" =>
        new TableOutputSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "FilterRows" =>
        new FilterRowsSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "DeleteRow" =>
        new DeleteRowSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "Delete" =>
        new DeleteSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "SparkMergeJoin" =>
        new MergeJoinSpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case "SparkGroupBy" =>
        new GroupBySpark(sparkSession,transMeta, stepMeta, copyNr, this, sparkBaseStepData,new EmptyMeta)
      case _ =>
        throw new RuntimeException(stepid+" not match")
    }
  }

  /**
    * Finds the RowSet between two steps (or copies of steps).
    *
    * @param from
    * the name of the "from" step
    * @param fromcopy
    * the copy number of the "from" step
    * @param to
    * the name of the "to" step
    * @param tocopy
    * the copy number of the "to" step
    * @return the row set, or null if none found
    */
  def findRowSet(from: String, fromcopy: Int, to: String, tocopy: Int): RowSet = { // Start with the transformation.
    var i = 0
    while ( {
      i < rowsets.size
    }) {
      val rs = rowsets.apply(i)
      if (rs.getOriginStepName.equalsIgnoreCase(from) && rs.getDestinationStepName.equalsIgnoreCase(to) && rs.getOriginStepCopy == fromcopy && rs.getDestinationStepCopy == tocopy) return rs

      {
        i += 1;
        i - 1
      }
    }
    null
  }

  /**
    * Stops all steps from running, and alerts any registered listeners.
    */
  def stopAll(): Unit = {
    if (steps == null) return
    var i = 0
    while ( {
      i < steps.size
    }) {
      val sid = steps.apply(i)
      val rt = sid.step
      rt.setStopped(true)
      rt.resumeRunning()
      // Cancel queries etc. by force...
      val si = rt
      try
        si.stopRunning(sid.meta, sid.data)
      catch {
        case e: Exception =>
          logError("Something went wrong while trying to stop the transformation: " + e.toString)
          logError(e.getMessage,e)
      }
      sid.data.setStatus(StepExecutionStatus.STATUS_STOPPED)

      {
        i += 1;
        i - 1
      }
    }
    // if it is stopped it is not paused
    paused.set(false)
    stopped.set(true)
    // Fire the stopped listener...
    transStoppedListeners.synchronized {
      for (listener <- transStoppedListeners) {
        //        listener.transStopped(this)
      }
    }


  }

  def addTransStoppedListener(transStoppedListener: TransStoppedListener): Unit = {
    transStoppedListeners.append(transStoppedListener)
  }

  /**
    * Checks whether the transformation is running.
    *
    * @return true if the transformation is running, false otherwise
    */
  def isRunning: Boolean = running

  /**
    * Sets whether the transformation is running.
    *
    * @param running
    * true if the transformation is running, false otherwise
    */
  def setRunning(running: Boolean): Unit = {
    this.running = running
  }
  def setAttributesMap(attributesMap: util.Map[String, util.Map[String, String]]): Unit = {
    this.attributesMap = attributesMap
  }

  def getAttributesMap: util.Map[String, util.Map[String, String]] = this.attributesMap

  def setAttribute(groupName: String, key: String, value: String): Unit = {
    var attributes = this.getAttributes(groupName)
    if (attributes == null) {
      attributes = new util.HashMap[String, String]
      this.attributesMap.put(groupName, attributes)
    }
    attributes.asInstanceOf[Map[String, String]].put(key, value)
  }

  def setAttributes(groupName: String, attributes: util.Map[String, String]): Unit = {
    this.attributesMap.put(groupName, attributes)
  }

  def getAttributes(groupName: String): util.Map[String, String] =
    this.attributesMap.get(groupName).asInstanceOf[util.Map[String, String]]

  def getAttribute(groupName: String, key: String): String = {
    val attributes = this.attributesMap.get(groupName).asInstanceOf[util.Map[_, _]]
    if (attributes == null) null
    else attributes.get(key).asInstanceOf[String]
  }

  /**
    *
    * @param boolean
    */
  def setIsStreaming(boolean: Boolean): Unit ={
    obj.synchronized{
      isStreaming = boolean
    }
  }

  /**
    *
    * @param boolean
    */
  def setIsIncrement(boolean: Boolean): Unit ={
    obj.synchronized{
      isIncrement = boolean
    }
  }
}
