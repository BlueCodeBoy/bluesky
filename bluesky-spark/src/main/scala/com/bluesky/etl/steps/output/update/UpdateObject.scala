package com.bluesky.etl.steps.output.update

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.alibaba.druid.pool.DruidDataSource
import com.bluesky.etl.common.CommonSparkUtils
import com.bluesky.etl.common.db.DruidCommonConnection
import com.bluesky.etl.common.jdbcdialect._
import com.bluesky.etl.core.trans.BaseMeta
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{getCommonJDBCType, insertStatement, logWarning}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.pentaho.di.core.database.DatabaseMeta
import org.pentaho.di.core.row.ValueMetaInterface
import org.pentaho.di.core.row.value.ValueMetaBase

import scala.collection.mutable
import scala.util.Try
import scala.util.control.Breaks.breakable
import scala.util.control.NonFatal

class UpdateObjectMeta(
                        val userName:String,
                        val pluginId:String,
                        val password:String,
                        val driver:String,
                        val url:String,
                       /** The lookup table name */
                       val schemaName:String,
                      /** The lookup table name */
                       val tableName:String,
                       /** database connection */
//                       val databaseMeta:DatabaseMeta,
                       /** which field in input stream to compare with? */
                       val keyStream:List[String],
                       /** field in table */
                       val keyLookup:List[String],
                       /** Comparator: =, <>, BETWEEN, ... */
                       val keyCondition:List[String],
                       /** Extra field for between... */
                       val keyStream2:List[String],
                       /** Field value to update after lookup */
                       val updateLookup:List[String],
                       /** Stream name to update value with */
                       val updateStream:List[String],
                       /** Commit size for inserts/updates */
                       val commitSize:Int,
                       /** update errors are ignored if this flag is set to true */
                       val errorIgnored:Boolean,
                       /** adds a boolean field to the output indicating success of the update */
                       val ignoreFlagField:String,
                       /** adds a boolean field to skip lookup and directly update selected fields */
                       val skipLookup:Boolean,
                       /** Flag to indicate the use of batch updates, enabled by default but disabled for backward compatibility */
                       val useBatchUpdate:Boolean
                           ) extends BaseMeta with Serializable{

}
case object UpdateObject extends Logging {
  //  val CR: String = System.getProperty("line.separator")

  /**
    *
    * @param objectMeta
    * @return
    */
  def applyConnection(objectMeta: UpdateObjectMeta, stepName: String) = {
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
    //    val prop = new java.util.Properties
    //    prop.setProperty("user",objectMeta.userName)
    //    prop.setProperty("password", objectMeta.password)
    //    prop.setProperty("driver", objectMeta.driver)
    //    JdbcUtils.createConnectionFactory(objectMeta.url,prop)
  }

  /**
    *
    * @param objectMeta
    * @param dataFrame
    */
  def updateData(objectMeta: UpdateObjectMeta, dataFrame: DataFrame, stepName: String): Unit = {
    dataFrame.foreachPartition(data => {
      if (!data.isEmpty) {
        val conn = UpdateObject.applyConnection(objectMeta, stepName)
        if (objectMeta.useBatchUpdate) {

          UpdateObject.updatePartition(conn, data.toIterator, objectMeta)
        } else {
          UpdateObject.updatePartitionSingleRow(conn, data.toIterator, objectMeta)
        }
      }
    })
  }

  /**
    *
    * @param getConnection
    * @param iterator
    * @param updateObjectMeta
    * @return
    */
  def updatePartitionSingleRow(
                                getConnection: () => Connection,
                                iterator: Iterator[Row],
                                updateObjectMeta: UpdateObjectMeta
                              ): Iterator[Byte] = {
    require(updateObjectMeta.commitSize >= 1,
      s"Invalid value `${updateObjectMeta.commitSize.toString}` for parameter " +
        s"`${JdbcUtils.JDBC_BATCH_INSERT_SIZE}`. The minimum value is 1.")

    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        logError(e.getMessage, e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      //      val stmt = updateStatement(conn , updateObjectMeta)
      //      try {
      var stmt: PreparedStatement = null
      var rowCount = 0
      var stindex = 0
      while (iterator.hasNext) {
        try {
          stindex = 0
          val row = iterator.next()
          stmt = updateStatement(conn, row, updateObjectMeta)
          if(null != stmt) {
            val numFields = updateObjectMeta.updateStream.length
            var i = 0
            var index = -1
            while (i < numFields) {
              if (null != row.getAs(updateObjectMeta.updateStream.apply(i))) {
                index = row.fieldIndex(updateObjectMeta.updateStream.apply(i))
                CommonSparkUtils.setDataByRow(row, index, stindex, stmt, updateObjectMeta.pluginId, conn)
                stindex += 1
              }
              i = i + 1
            }
            var j = 0
            while (j < updateObjectMeta.keyStream.length) {
              index = row.fieldIndex(updateObjectMeta.keyStream.apply(j))
              CommonSparkUtils.setDataByRow(row, index, stindex, stmt, updateObjectMeta.pluginId, conn)
              j += 1
              i += 1
              stindex += 1
            }
            var k = 0
            //          while (k < updateObjectMeta.keyStream2.length){
            //            index  = row.fieldIndex(updateObjectMeta.keyStream2.apply(k))
            //            setDataByRow(row,index,i,stmt,updateObjectMeta.pluginId,conn)
            //            k+=1
            //            i+=1
            //          }
            //          stmt.addBatch()
            //          rowCount += 1
            //          if (rowCount % updateObjectMeta.commitSize == 0) {
            //            stmt.executeBatch()
            //            rowCount = 0
            //          }
            stmt.executeUpdate()
          }
        } finally {
          if (null != stmt) {
            stmt.close()
          }
        }
      }

      //        if (rowCount > 0) {
      //          stmt.executeBatch()
      //        }
      //      } finally {
      //        stmt.close()
      //      }
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
            logError(e.getMessage, e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
    *
    * @param getConnection
    * @param iterator
    * @param updateObjectMeta
    * @return
    */
  def updatePartition(
                       getConnection: () => Connection,
                       iterator: Iterator[Row],
                       updateObjectMeta: UpdateObjectMeta
                     ): Iterator[Byte] = {
    require(updateObjectMeta.commitSize >= 1,
      s"Invalid value `${updateObjectMeta.commitSize.toString}` for parameter " +
        s"`${JdbcUtils.JDBC_BATCH_INSERT_SIZE}`. The minimum value is 1.")

    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        logError(e.getMessage, e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val stmt = updateStatement(conn, updateObjectMeta)
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = updateObjectMeta.updateStream.length
          var i = 0
          var index = -1
          while (i < numFields) {
            index = row.fieldIndex(updateObjectMeta.updateStream.apply(i))
            CommonSparkUtils.setDataByRow(row, index, i, stmt, updateObjectMeta.pluginId, conn)

            i = i + 1
          }
          var j = 0
          while (j < updateObjectMeta.keyStream.length) {
            index = row.fieldIndex(updateObjectMeta.keyStream.apply(j))
            CommonSparkUtils.setDataByRow(row, index, i, stmt, updateObjectMeta.pluginId, conn)
            j += 1
            i += 1
          }
          var k = 0
          //          while (k < updateObjectMeta.keyStream2.length){
          //            index  = row.fieldIndex(updateObjectMeta.keyStream2.apply(k))
          //            setDataByRow(row,index,i,stmt,updateObjectMeta.pluginId,conn)
          //            k+=1
          //            i+=1
          //          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % updateObjectMeta.commitSize == 0) {
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
            logError(e.getMessage, e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
    *
    * Returns a PreparedStatement that inserts a row into table via conn.   null condition
    * @param conn
    * @param row
    * @param updateObjectMeta
    * @return
    */
  def updateStatement(conn: Connection, row: Row,
                      updateObjectMeta: UpdateObjectMeta
                     )
  : PreparedStatement = {
    val jdbcDialect = CommonSparkUtils.getJdbcDialect(updateObjectMeta.pluginId)
    var sql: mutable.StringBuilder = new mutable.StringBuilder(s"UPDATE ${updateObjectMeta.tableName}  SET ")
    var i: Int = 0
    var first: Boolean = true
    while ( {
      i < updateObjectMeta.updateLookup.length
    }) {
      if (null != row.getAs(updateObjectMeta.updateStream.apply(i))) {
        if (!first) {
          sql.append(",   ")
        }
        sql.append(jdbcDialect.quoteIdentifier(updateObjectMeta.updateLookup.apply(i)))
        sql.append(s" = ? ")
        first = false
      }
      i += 1
    }

    sql.append("WHERE ")

    var j = 0
    while ( {
      j < updateObjectMeta.keyLookup.length
    }) {
      if (j != 0) {
        sql.append("AND   ")
      }
      sql.append(" ( ( ")
      sql.append(jdbcDialect.quoteIdentifier(updateObjectMeta.keyLookup.apply(j)))
      if ("BETWEEN".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j))) {
        sql.append(" BETWEEN ? AND ? ")
      } else if ("IS NULL".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j)) || "IS NOT NULL".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j))) {
        sql.append(s" ${updateObjectMeta.keyCondition.apply(j)} ")
      }
      else if ("= ~NULL".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j))) {
        sql.append(" IS NULL AND ")
        //        if (jdbcDialect.requiresCastToVariousForIsNull){
        //          sql.append("CAST(? AS VARCHAR(256)) IS NULL")
        //        }
        //        else {
        sql.append("? IS NULL")
        //        }
        // null check
        sql.append(s" ) OR (  ${jdbcDialect.quoteIdentifier(updateObjectMeta.keyLookup.apply(j))} = ?")
        // equality check, cloning so auto-rename because of adding same fieldname does not cause problems
      } else {
        sql.append(s" ${updateObjectMeta.keyCondition.apply(j)} ? ")
      }
      sql.append(" ) ) ")

      j += 1
    }
    if(first){
      null
    }else{
      conn.prepareStatement(sql.toString())
    }
  }

  /**
    * Returns a PreparedStatement that inserts a row into table via conn.
    * @param conn
    * @param updateObjectMeta
    * @return
    */
  def updateStatement(conn: Connection,

                      updateObjectMeta: UpdateObjectMeta
                     )
  : PreparedStatement = {
    val jdbcDialect = CommonSparkUtils.getJdbcDialect(updateObjectMeta.pluginId)
    var sql: mutable.StringBuilder = new mutable.StringBuilder(s"UPDATE ${updateObjectMeta.tableName}  SET ")
    var i: Int = 0
    while ( {
      i < updateObjectMeta.updateLookup.length
    }) {
      if (i != 0) {
        sql.append(",   ")
      }
      sql.append(jdbcDialect.quoteIdentifier(updateObjectMeta.updateLookup.apply(i)))
      sql.append(s" = ? ")
      i += 1
    }

    sql.append("WHERE ")

    var j = 0
    while ( {
      j < updateObjectMeta.keyLookup.length
    }) {
      if (j != 0) {
        sql.append("AND   ")
      }
      sql.append(" ( ( ")
      sql.append(jdbcDialect.quoteIdentifier(updateObjectMeta.keyLookup.apply(j)))
      if ("BETWEEN".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j))) {
        sql.append(" BETWEEN ? AND ? ")
      } else if ("IS NULL".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j)) || "IS NOT NULL".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j))) {
        sql.append(s" ${updateObjectMeta.keyCondition.apply(j)} ")
      }
      else if ("= ~NULL".equalsIgnoreCase(updateObjectMeta.keyCondition.apply(j))) {
        sql.append(" IS NULL AND ")
        //        if (jdbcDialect.requiresCastToVariousForIsNull){
        //          sql.append("CAST(? AS VARCHAR(256)) IS NULL")
        //        }
        //        else {
        sql.append("? IS NULL")
        //        }
        // null check
        sql.append(s" ) OR (  ${jdbcDialect.quoteIdentifier(updateObjectMeta.keyLookup.apply(j))} = ?")
        // equality check, cloning so auto-rename because of adding same fieldname does not cause problems
      } else {
        sql.append(s" ${updateObjectMeta.keyCondition.apply(j)} ? ")
      }
      sql.append(" ) ) ")

      j += 1
    }
    conn.prepareStatement(sql.toString())
  }


}
