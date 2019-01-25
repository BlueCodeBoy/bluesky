package com.bluesky.etl.core.trans

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.bluesky.etl.core.queue.RowSet
import org.apache.spark.sql.{Dataset, Row}
import org.pentaho.di.core.Const
import org.pentaho.di.core.row.RowMetaInterface

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 18-8-14.
  */
abstract class BaseRowSet extends RowSet with Comparable[RowSet] {
  protected var rowMeta: RowMetaInterface = null
  protected var done: AtomicBoolean = new AtomicBoolean(false)
  protected var originStepName: String = null
  protected var originStepCopy: AtomicInteger = new AtomicInteger(0)
  protected var destinationStepName: String = null
  protected var destinationStepCopy: AtomicInteger = new AtomicInteger(0)

  protected var remoteSlaveServerName: String = null


  /**
    *
    * @param rowSet
    * @return
    */
  def equals(rowSet: BaseRowSet): Boolean = compareTo(rowSet) == 0

  /**
    *
    * Compares using the target steps and copy, not the source. That way, re-partitioning is always done in the same way.
    *
    * @param rowSet
    * @return
    */
  override def compareTo(rowSet: RowSet): Int = {
    val target = remoteSlaveServerName + "." + destinationStepName + "." + destinationStepCopy.intValue
    val comp = rowSet.getRemoteSlaveServerName + "." + rowSet.getDestinationStepName + "." + rowSet.getDestinationStepCopy
    target.compareTo(comp)
  }

  override def synchronizedPutRow(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

  /**
    *
    * @param rowMeta
    * The description of the row data
    * @param rowData
    * the row of data
    * @return true if the row was successfully added to the rowset and false if this buffer was full.
    */

  override def putRow(rowMeta: RowMetaInterface, rowData: Dataset[Row]): Boolean

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
  override def putRowWait(rowMeta: RowMetaInterface, rowData: Dataset[Row], time: Long, tu: TimeUnit): Boolean

  /**
    *
    * @return a row of data or null if no row is available.
    */
  override def getRow: Dataset[Row]

  /**
    *
    * @return a row of data or null if no row is available.
    */
  override def getRowImmediate: Dataset[Row]

  /**
    *
    * @param timeout
    * @param tu
    * @return a row of data or null if no row is available.
    */
  override def getRowWait(timeout: Long, tu: TimeUnit): Dataset[Row]

  /**
    *
    */
  override def setDone(): Unit = {
    done.set(true)
  }

  /**
    *
    * @return Returns true if there is no more input and vice versa
    */
  override def isDone: Boolean = done.get

  /**
    *
    * @return Returns the originStepName.
    */
  override def getOriginStepName: String = {
    originStepName synchronized
      originStepName

  }

  /**
    *
    * @return Returns the originStepCopy.
    */
  override def getOriginStepCopy: Int = originStepCopy.get

  /**
    *
    * @return Returns the destinationStepName.
    */
  override def getDestinationStepName: String = destinationStepName

  /**
    *
    * @return Returns the destinationStepCopy.
    */
  override def getDestinationStepCopy: Int = destinationStepCopy.get

  /**
    *
    * @return
    */
  override def getName: String = toString

  /**
    *
    * @return
    */
  override def toString: String = {
    var str: ArrayBuffer[String] = null
    originStepName.synchronized {
      str = new ArrayBuffer[String]()
      str.append(originStepName)
    }

    str.append(".")
    originStepCopy.synchronized {
      str.append(originStepCopy.toString)
    }

    str.append(" - ")
    destinationStepName.synchronized {
      str.append(destinationStepName)
    }

    str.append(".")
    destinationStepCopy.synchronized {
      str.append(destinationStepCopy.toString)
    }

    if (!Const.isEmpty(remoteSlaveServerName)) {
      remoteSlaveServerName.synchronized {

        str.append(" (")
        str.append(remoteSlaveServerName)
        str.append(")")
      }

    }
    str.mkString("")
  }

  /**
    *
    * @return Return the size (or max capacity) of the RowSet
    */
  override def size: Int

  /**
    *
    * @param from
    * @param from_copy
    * @param to
    * @param to_copy
    */
  override def setThreadNameFromToCopy(from: String, from_copy: Int, to: String, to_copy: Int): Unit = {
    if (originStepName == null) {
      originStepName = from
    } else {
      originStepName.synchronized {
        originStepName = from
      }
    }
    originStepCopy.set(from_copy)
    if (destinationStepName == null) {
      destinationStepName = to
    }
    else {
      destinationStepName.synchronized {
        destinationStepName = to
      }
    }
    destinationStepCopy.set(to_copy)
  }

  /**
    *
    * @return the rowMeta
    */
  override def getRowMeta: RowMetaInterface = rowMeta

  /**
    *
    * @param rowMeta
    * the rowMeta to set
    */
  override def setRowMeta(rowMeta: RowMetaInterface): Unit = {
    this.rowMeta = rowMeta
  }

  /**
    *
    * @return the targetSlaveServer
    */
  override def getRemoteSlaveServerName: String = remoteSlaveServerName

  /**
    *
    * @param remoteSlaveServerName
    * the remote slave server to set
    */
  override def setRemoteSlaveServerName(remoteSlaveServerName: String): Unit = {
    this.remoteSlaveServerName = remoteSlaveServerName
  }

  /**
    * By default we don't report blocking, only for monitored transformations.
    *
    * @return true if this row set is blocking on reading or writing.
    */
  override def isBlocking = false

}
