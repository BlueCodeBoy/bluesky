package com.bluesky.etl.steps.output.tableoutput

import java.sql.{Connection, PreparedStatement}

import com.alibaba.druid.pool.DruidDataSource
import com.bluesky.etl.common.CommonSparkUtils
import com.bluesky.etl.common.db.DruidCommonConnection
import com.bluesky.etl.core.trans.BaseMeta
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.util.control.Breaks.breakable
import scala.util.control.NonFatal

class TableOutputObjectMeta(
//                             val databaseMeta:DatabaseMeta,
    val userName:String,
    val pluginId:String,
    val password:String,
    val driver:String,
    val url:String,
                            val schemaName:String,
                            val  tableName:String,
                            val  commitSize:Int,
                            val  truncateTable:Boolean,
                            val  ignoreErrors:Boolean,
                            val  useBatchUpdate:Boolean,
                            val  tableNameInField:Boolean,
                            val  tableNameField:String,
                            val tableNameInTable:Boolean,
                            val returningGeneratedKeys:Boolean,
                            val generatedKeyField:String,
                            val specifyFields:Boolean,
                            val fieldStream:List[String],
                            val fieldDatabase:List[String]
                           ) extends BaseMeta with Serializable{


}
object TableOutputObject extends Logging {
  val obj = new Object()
  /**
    *
    * @param tableOutputObjectMeta
    * @param dataFrame
    */
  def startSaveData(tableOutputObjectMeta:TableOutputObjectMeta,dataFrame: DataFrame,stepName:String):Unit={
    dataFrame.foreachPartition(data=>{
      if(!data.isEmpty){
        val conn = applyConnection(tableOutputObjectMeta,stepName)
        savePartition(conn,data, tableOutputObjectMeta)
      }
    })
  }
  /**
    *
    * @param objectMeta
    * @return
    */
  def applyConnection(objectMeta: TableOutputObjectMeta,stepName:String)  = {
    obj.synchronized {
      var ds: DruidDataSource = DruidCommonConnection.getDataSourceByuuid(stepName)

      if (null == ds) {
        ds = DruidCommonConnection.createConnectionPool(objectMeta.driver, objectMeta.url, objectMeta.userName, objectMeta.password, objectMeta.pluginId, stepName)
      }
      () => {
        var conn:Connection = null
        breakable {
          while ( {
            (conn = DruidCommonConnection.getConnect(ds)) == null
          }) {
              if(null!=conn){
                scala.util.control.Breaks.break()
              }
          }
        }
        conn
      }
    }
//    val prop = new java.util.Properties
//    prop.setProperty("user",objectMeta.userName)
//    prop.setProperty("password", objectMeta.password)
//    prop.setProperty("driver", objectMeta.driver)
//    JdbcUtils.createConnectionFactory(objectMeta.url,prop)

  }

//  /**
//    *
//    * @param objectMeta
//    * @param dataFrame
//    */
//  def saveData(objectMeta: TableOutputObjectMeta,dataFrame: DataFrame):Unit = {
//    dataFrame.foreachPartition(data=>{
//      if(!data.isEmpty){
//        val conn = TableOutputObject.applyConnection(objectMeta)
//          TableOutputObject.savePartition(conn,data.toIterator, objectMeta)
//      }
//    })
//  }

  def savePartition(
                       getConnection: () => Connection,
                       iterator: Iterator[Row],
                       tableOutputObjectMeta: TableOutputObjectMeta
                     ): Iterator[Byte] = {
    require(tableOutputObjectMeta.commitSize >= 1,
      s"Invalid value `${tableOutputObjectMeta.commitSize.toString}` for parameter " +
        s"`${JdbcUtils.JDBC_BATCH_INSERT_SIZE}`. The minimum value is 1.")

    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        logError(e.getMessage,e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      var stmt: PreparedStatement = null
      try {
        var rowCount = 0
        var row: Row = null
        var numFields: Int = 0
        var i = 0
        var index = -1
        while (iterator.hasNext) {
          row = iterator.next()
          if (null == stmt) {
            stmt = insertStatement(conn, tableOutputObjectMeta)
          }
          numFields = tableOutputObjectMeta.fieldDatabase.length
          i = 0
          index = -1
          while (i < numFields) {
            index = row.fieldIndex(tableOutputObjectMeta.fieldStream.apply(i))
            CommonSparkUtils.setDataByRow(row, index, i, stmt, tableOutputObjectMeta.pluginId, conn)

            i = i + 1
          }
          //          while (k < updateObjectMeta.keyStream2.length){
          //            index  = row.fieldIndex(updateObjectMeta.keyStream2.apply(k))
          //            setDataByRow(row,index,i,stmt,updateObjectMeta.pluginId,conn)
          //            k+=1
          //            i+=1
          //          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % tableOutputObjectMeta.commitSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }

        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception =>
            logError(e.getMessage,e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
    * Returns a PreparedStatement that inserts a row into table via conn.
    */
  def insertStatement(conn: Connection,
                      tableOutputObjectMeta: TableOutputObjectMeta

                     )
  : PreparedStatement = {
    val jdbcDialect =  CommonSparkUtils.getJdbcDialect(tableOutputObjectMeta.pluginId)
    var sql:mutable.StringBuilder = new mutable.StringBuilder(s"insert into  ${tableOutputObjectMeta.tableName}  ( ")
    var i:Int = 0
    while ( {
      i < tableOutputObjectMeta.fieldDatabase.length
    }) {
      if (i != 0){
        sql.append(",   ")
      }
      sql.append(jdbcDialect.quoteIdentifier(tableOutputObjectMeta.fieldDatabase.apply(i)))
      i += 1
    }
    i= 0
    sql.append(") VALUES (")
    while ( {
      i < tableOutputObjectMeta.fieldDatabase.length
    }) {
      if (i != 0){
        sql.append(",   ")
      }
      sql.append(" ? ")
      i += 1
    }
    sql.append(')')
    conn.prepareStatement(sql.toString())
  }/**
    *
    * @param objectMeta
    * @param dataFrame
    */
//  def saveData(objectMeta: TableOutputObjectMeta,dataFrame: DataFrame):Unit = {
//    val prop = new java.util.Properties
////    prop.setProperty("user",objectMeta.databaseMeta.getUsername)
////    prop.setProperty("password", objectMeta.databaseMeta.getPassword)
////    prop.setProperty("driver", objectMeta.databaseMeta.getDriverClass)
//    var fdataFrame:DataFrame =dataFrame
//    if(objectMeta.specifyFields){
//      for(index <- 0 until objectMeta.fieldStream.length){
//       fdataFrame =  fdataFrame.withColumnRenamed(objectMeta.fieldStream.apply(index),objectMeta.fieldDatabase.apply(index))
//      }
//    }
//    if(objectMeta.truncateTable){
//      fdataFrame.select(objectMeta.fieldDatabase.map(col):_*).write.mode(SaveMode.Overwrite).jdbc(objectMeta.databaseMeta.getURL, objectMeta.tableName, prop)
//    }else{
//      fdataFrame.select(objectMeta.fieldDatabase.map(col):_*).write.mode(SaveMode.Append).jdbc(objectMeta.databaseMeta.getURL, objectMeta.tableName, prop)
//    }
//  }
}
