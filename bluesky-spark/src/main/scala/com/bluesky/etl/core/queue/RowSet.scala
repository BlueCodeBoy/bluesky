package com.bluesky.etl.core.queue

import java.util.concurrent.TimeUnit

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}
import org.pentaho.di.core.row.RowMetaInterface

/**
  * Created by root on 18-8-14.
  */
trait RowSet {

  /**
    * Offer a row of data to this rowset providing for the description (metadata) of the row. If the buffer is full, wait
    * (block) for a small period of time.
    *
    * @param rowMeta
    * The description of the row data
    * @param rowData
    * the row of data
    * @return true if the row was successfully added to the rowset and false if this buffer was full.
    */
  def putRow(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

  /**
    *
    * @param rowMeta
    * @param data
    * @return
    */
  def putBroadCastRow(rowMeta: RowMetaInterface, data:(StructType,Broadcast[List[Row]])): Boolean

  /**
    *
    * @param addTime
    * @param rowMeta
    * @param rowData
    * @return
    */
  def putRowWithTime(addTime:Long, successTime:Long, rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

  /**
    *
    * @param stepName
    * @param rowMeta
    * @param rowData
    * @return
    */
  def putRowToError(msg:String,stepId:String,stepName:String,rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

  /**
    *
    * @param rowMeta
    * @param rowData
    * @return
    */
  def putRowBlock(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

  /**
    *
    * @param rowMeta
    * @param rowData
    * @return
    */
  def synchronizedPutRow(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

  /**
    * Offer a row of data to this rowset providing for the description (metadata) of the row. If the buffer is full, wait
    * (block) for a period of time defined in this call.
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
  def putRowWait(rowMeta: RowMetaInterface, rowData: Dataset[Row], time: Long, tu: TimeUnit): Boolean

  /**
    * Get a row from the input buffer, it blocks for a short period until a new row becomes available. Otherwise, it
    * returns null.
    *
    * @return a row of data or null if no row is available.
    */
  def getRow: Dataset[Row]
  def getBroadCastRow:(StructType,Broadcast[List[Row]])
  /**
    * Get the first row in the list immediately.
    *
    * @return a row of data or null if no row is available.
    */
  def getRowImmediate: Dataset[Row]

  /**
    *
    * @return
    */
  def getRowBLock: Dataset[Row]

  /**
    *
    * @return
    */
  def getRowWithTimeLock: (Long,Long,Dataset[Row])
  def getRowWithTimeWait(timeout: Long, tu: TimeUnit):  (Long,Long,Dataset[Row])

  /**
    *
    * @return
    */
  def getErrorRowLock: (String,String,String,Dataset[Row])
  def getErrorRowWait(timeout: Long, tu: TimeUnit): (String,String,String,Dataset[Row])

  /**
    * get the first row in the list immediately if it is available or wait until timeout
    *
    * @return a row of data or null if no row is available.
    */
  def getRowWait(timeout: Long, tu: TimeUnit): Dataset[Row]

  /**
    * @return Set indication that there is no more input
    */
  def setDone(): Unit

  /**
    * @return Returns true if there is no more input and vice versa
    */
  def isDone: Boolean

  /**
    * @return Returns the originStepName.
    */
  def getOriginStepName: String

  /**
    * @return Returns the originStepCopy.
    */
  def getOriginStepCopy: Int

  /**
    * @return Returns the destinationStepName.
    */
  def getDestinationStepName: String

  /**
    * @return Returns the destinationStepCopy.
    */
  def getDestinationStepCopy: Int

  def getName: String

  /**
    *
    * @return Return the size (or max capacity) of the RowSet
    */
  def size: Int

  /**
    * This method is used only in Trans.java when created RowSet at line 333. Don't need any synchronization on this
    * method
    *
    */
  def setThreadNameFromToCopy(from: String, from_copy: Int, to: String, to_copy: Int): Unit

  /**
    * @return the rowMeta
    */
  def getRowMeta: RowMetaInterface

  /**
    * @param rowMeta
    * the rowMeta to set
    */
  def setRowMeta(rowMeta: RowMetaInterface): Unit

  /**
    * @return the targetSlaveServer
    */
  def getRemoteSlaveServerName: String

  /**
    * @param remoteSlaveServerName
    * the remote slave server to set
    */
  def setRemoteSlaveServerName(remoteSlaveServerName: String): Unit

  /**
    * @return true if this row set is blocking.
    */
  def isBlocking: Boolean

  /**
    * Clear this rowset: remove all rows and remove the "done" flag.
    */
  def clear(): Unit
}
