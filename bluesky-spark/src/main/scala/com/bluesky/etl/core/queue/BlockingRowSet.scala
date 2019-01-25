package com.bluesky.etl.core.queue

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import com.bluesky.etl.core.trans.BaseRowSet
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}
import org.pentaho.di.core.Const
import org.pentaho.di.core.row.RowMetaInterface

/**
  * Created by root on 18-8-14.
  */
class BlockingRowSet(maxSize: Int) extends BaseRowSet {

  private var broadCast:(StructType,Broadcast[List[Row]]) = null
  private val stateLock = new ReentrantLock
  protected var queArray: BlockingQueue[Dataset[Row]] = new ArrayBlockingQueue[Dataset[Row]](maxSize, false)
  protected var queArrayWithTime: BlockingQueue[(Long,Long,Dataset[Row])] = new ArrayBlockingQueue[(Long,Long,Dataset[Row])](maxSize, false)
  protected var errorQueArray: BlockingQueue[(String,String,String,Dataset[Row])] = new ArrayBlockingQueue[(String,String,String,Dataset[Row])](maxSize, false)

  protected var timeoutGet = Const.toInt(System.getProperty(Const.KETTLE_ROWSET_GET_TIMEOUT), Const.TIMEOUT_GET_MILLIS)
  protected var timeoutPut = Const.toInt(System.getProperty(Const.KETTLE_ROWSET_PUT_TIMEOUT), Const.TIMEOUT_PUT_MILLIS)


  /**
    *
    * @param rowMeta
    * @param data
    * @return
    */
  override def putBroadCastRow(rowMeta: RowMetaInterface, data: (StructType, Broadcast[List[Row]])): Boolean = {
    this.rowMeta = rowMeta
    try {
      if(null == broadCast){
        broadCast = data
      }
      true
    } catch {
      case e: InterruptedException =>
        false
      case e: NullPointerException =>
        false
    }
  }

  override def getBroadCastRow: (StructType, Broadcast[List[Row]]) = broadCast

  /**
    *
    * @param stepName
    * @param rowMeta
    * @param rowData
    * @return
    */
  override def putRowToError(msg:String,stepId:String,stepName: String, rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean = {
    this.rowMeta = rowMeta
    try {
      errorQueArray.offer((msg,stepId,stepName, rowData), timeoutPut, TimeUnit.MILLISECONDS)
    } catch {
      case e: InterruptedException =>
        false
      case e: NullPointerException =>
        false
    }
  }

  /**
    *
    * @return
    */
  override def getErrorRowLock: (String,String,String, Dataset[Row]) = {
    errorQueArray.take()
  }
  /**
    *
    * @param timeout
    * @param tu
    * @return a row of data or null if no row is available.
    */
  override def getErrorRowWait(timeout: Long, tu: TimeUnit): (String,String,String, Dataset[Row]) ={
    try {

      errorQueArray.poll(timeout, tu)
    }
    catch {
      case e: InterruptedException =>
        null
    }
  }


  /**
    *
    * @param addTime
    * @param rowMeta
    * @param rowData
    * @return
    */
  override def putRowWithTime(addTime: Long, successTime:Long,  rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean = {
    this.rowMeta = rowMeta
    try {
      queArrayWithTime.offer((addTime,successTime, rowData), timeoutPut, TimeUnit.MILLISECONDS)
    } catch {
      case e: InterruptedException =>
        false
      case e: NullPointerException =>
        false
    }
  }

  /**
    *
    * @return
    */
  override def getRowWithTimeLock: (Long,Long, Dataset[Row]) ={
    queArrayWithTime.take()
  }
  override def getRowWithTimeWait(timeout: Long, tu: TimeUnit):  (Long,Long,Dataset[Row])={
    try {

      queArrayWithTime.poll(timeout, tu)
    }
    catch {
      case e: InterruptedException =>
        null
    }
  }

  /**
    *
    * @param rowMeta
    * The description of the row data
    * @param rowData
    * the row of data
    * @return true if the row was successfully added to the rowset and false if this buffer was full.
    */
  override def putRow(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean
  = putRowWait(rowMeta, rowData, timeoutPut, TimeUnit.MILLISECONDS)

  /**
    *
    * @param rowMeta
    * @param rowData
    * @return
    */
  override def synchronizedPutRow(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean ={
    var res:Boolean = false
    stateLock.synchronized{
     res = putRowWait(rowMeta, rowData, timeoutPut, TimeUnit.MILLISECONDS)
    }
    res
  }

  /**
    *
    * @param rowMeta
    * @param rowData
    * @return
    */
  override def putRowBlock(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean = {
    this.rowMeta = rowMeta
    try {
      queArray.put(rowData)
      true
    } catch {
      case e: InterruptedException =>
        false
      case e: NullPointerException =>
        false
    }
  }

  /**
    *
    * @param rowMeta
    * The description of the row data
    * @param rowData
    * the row of data
    * @param time
    * The number of units of time
    * @param tu
    * The unit of time to use
    * @return true if the row was successfully added to the rowset and false if this buffer was full.
    */
  override def putRowWait(rowMeta: RowMetaInterface, rowData: Dataset[Row], time: Long, tu: TimeUnit): Boolean = {
    this.rowMeta = rowMeta
    try {
      queArray.offer(rowData, time, tu)
    } catch {
      case e: InterruptedException =>
        false
      case e: NullPointerException =>
        false
    }
  }

  /**
    *
    * @return
    */
  override def getRowBLock: Dataset[Row] ={
    queArray.take()
  }

  /**
    *
    * @return a row of data or null if no row is available.
    */
  override def getRow: Dataset[Row] = getRowWait(timeoutGet, TimeUnit.MILLISECONDS)

  /**
    *
    * @param timeout
    * @param tu
    * @return a row of data or null if no row is available.
    */
  override def getRowWait(timeout: Long, tu: TimeUnit): Dataset[Row] ={

      try {
        queArray.poll(timeout, tu)
      }
    catch {
      case e: InterruptedException =>
        null
    }
  }

  /**
    *
    * @return a row of data or null if no row is available.
    */
  override def getRowImmediate: Dataset[Row] = queArray.poll

  /**
    *
    * @return Return the size (or max capacity) of the RowSet
    */
  override def size: Int = queArray.size

  /**
    *
    */
  override def clear(): Unit = {
    queArray.clear()
    done.set(false)
  }
}
