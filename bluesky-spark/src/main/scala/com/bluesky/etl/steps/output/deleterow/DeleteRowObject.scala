package com.bluesky.etl.steps.output.deleterow

import java.sql.{Connection, PreparedStatement}

import com.bluesky.etl.common.jdbcdialect._
import com.bluesky.etl.core.trans.BaseMeta
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.util.control.NonFatal

class DeleteRowObjectMeta(
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
                       val keyStream:List[String],
                       /** field in table */
                       val keyLookup:List[String],
                       /** Comparator: =, <>, BETWEEN, ... */
                       val keyCondition:List[String],
                       /** Extra field for between... */
                       val keyStream2:List[String]
                           ) extends BaseMeta with Serializable{

}
case object DeleteRowObject {
  val CR: String = System.getProperty("line.separator")

  /**
    *
    * @param objectMeta
    * @return
    */
  def applyConnection(objectMeta: DeleteRowObjectMeta)  = {
    val prop = new java.util.Properties
    prop.setProperty("user",objectMeta.userName)
    prop.setProperty("password", objectMeta.password)
    prop.setProperty("driver", objectMeta.driver)
    JdbcUtils.createConnectionFactory(objectMeta.url,prop)
  }

  /**
    *
    * @param objectMeta
    * @param dataFrame
    */
  def deleteData(objectMeta: DeleteRowObjectMeta,dataFrame: DataFrame):Unit = {
    dataFrame.foreachPartition(data=>{
      if(!data.isEmpty){
        val conn = DeleteRowObject.applyConnection(objectMeta)
        DeleteRowObject.deleteDatabByPartition(conn,data.toIterator,
          objectMeta)
      }
    })
  }
  def deleteDatabByPartition(
                     getConnection: () => Connection,
                     iterator: Iterator[Row],
                     deleteRowObjectMeta: DeleteRowObjectMeta
                     ): Iterator[Byte] = {

    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        println("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val stmt = deleteStatement(conn ,deleteRowObjectMeta )
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var j = 0
          var index = 0
          while (j < deleteRowObjectMeta.keyStream.length){
            index  = row.fieldIndex(deleteRowObjectMeta.keyStream.apply(j))
            setDataByRow(row,index,j,stmt,deleteRowObjectMeta.pluginId,conn)
            j+=1
          }
          var k = 0
//          while (k < updateObjectMeta.keyStream2.length){
//            index  = row.fieldIndex(updateObjectMeta.keyStream2.apply(k))
//            setDataByRow(row,index,i,stmt,updateObjectMeta.pluginId,conn)
//            k+=1
//            i+=1
//          }
          stmt.executeUpdate()
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
          case e: Exception => println("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
    *
    * @param row
    * @param index
    * @param i
    * @param stmt
    * @param pluginId
    * @param conn
    */
  def setDataByRow(row: Row,index:Int,i:Int,stmt:PreparedStatement,pluginId:String,conn:Connection):Unit={
    row.schema.fields(index).dataType match {
    case IntegerType => stmt.setInt(i + 1, row.getInt(index))
    case LongType => stmt.setLong(i + 1, row.getLong(index))
    case DoubleType => stmt.setDouble(i + 1, row.getDouble(index))
    case FloatType => stmt.setFloat(i + 1, row.getFloat(index))
    case ShortType => stmt.setInt(i + 1, row.getShort(index))
    case ByteType => stmt.setInt(i + 1, row.getByte(index))
    case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(index))
    case StringType => stmt.setString(i + 1, row.getString(index))
    case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](index))
    case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](index))
    case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](index))
    case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(index))
    case ArrayType(et, _) =>
    // remove type length parameters from end of type name
    val typeName = getJdbcType(et,getJdbcDialect(pluginId)).databaseTypeDefinition
    .toLowerCase.split("\\(")(0)
    val array = conn.createArrayOf(
    typeName,
    row.getSeq[AnyRef](index).toArray)
    stmt.setArray(i + 1, array)
    case _ => throw new IllegalArgumentException(
    s"Can't translate non-null value for field $i")
    //              }
  }
  }
  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }
  /**
    * Returns a PreparedStatement that inserts a row into table via conn.
    */
  def deleteStatement(conn: Connection,
                      deleteRowObjectMeta: DeleteRowObjectMeta
                     )
  : PreparedStatement = {
    val jdbcDialect =  getJdbcDialect(deleteRowObjectMeta.pluginId)
    var sql:mutable.StringBuilder = new mutable.StringBuilder(s"delete from  ${deleteRowObjectMeta.tableName} $CR  ")

    sql.append("WHERE ")

    var j = 0
    while ( {
      j < deleteRowObjectMeta.keyLookup.length
    }) {
      if (j != 0){
        sql.append("AND   ")
      }
      sql.append(" ( ( ")
      sql.append(jdbcDialect.quoteIdentifier(deleteRowObjectMeta.keyLookup.apply(j)))
      if ("BETWEEN".equalsIgnoreCase(deleteRowObjectMeta.keyCondition.apply(j))) {
        sql.append(" BETWEEN ? AND ? ")
      } else if ("IS NULL".equalsIgnoreCase(deleteRowObjectMeta.keyCondition.apply(j)) || "IS NOT NULL".equalsIgnoreCase(deleteRowObjectMeta.keyCondition.apply(j))){
        sql .append(s" ${deleteRowObjectMeta.keyCondition.apply(j)} ")
      }
      else if ("= ~NULL".equalsIgnoreCase(deleteRowObjectMeta.keyCondition.apply(j))) {
        sql.append(" IS NULL AND ")
//        if (jdbcDialect.requiresCastToVariousForIsNull){
//          sql.append("CAST(? AS VARCHAR(256)) IS NULL")
//        }
//        else {
          sql.append("? IS NULL")
//        }
        // null check
        sql.append(s" ) OR (  ${jdbcDialect.quoteIdentifier(deleteRowObjectMeta.keyLookup.apply(j))} = ?")
        // equality check, cloning so auto-rename because of adding same fieldname does not cause problems
      } else {
        sql .append(s" ${deleteRowObjectMeta.keyCondition.apply(j)} ? ")
      }
      sql .append(" ) ) ")

        j += 1
    }
    conn.prepareStatement(sql.toString())
  }
  def getJdbcDialect( pluginId:String):JdbcDialect={
    pluginId match {
      case "ORACLE"=>
        OracleDialect
      case "POSTGRESQL"=>
        PostgresDialect
      case "DERBY"=>
        DerbyDialect
      case "MYSQL"=>
        MySQLDialect
      case "MSSQLNATIVE"=>
        MsSqlServerDialect
      case "MSSQL"=>
        MsSqlServerDialect

      case "DB2"=>
        DB2Dialect
      //        INGRES
      //        EXASOL4
      //        HYPERSONIC
      //        AS/400
      //        ORACLERDB
      //        INTERBASE
      //        INFOBRIGHT
      //        DBASE
      //        KINGBASEES
      //    EXTENDB
      //    MSACCESS
      //    SYBASE
      //    HIVE
      //    SAPR3
      //    SQLBASE
      //    INFORMIX
      //    LucidDB
      //    VERTICA
      //    INFINIDB
      //    HIVE2
      //    TERADATA
      //    UNIVERSE
      //    MONETDB
      //
      //    H2
      //    CACHE
      //
      //    GREENPLUM
      //    SAPDB
      //    GENERIC
      //    VECTORWISE
      //    SQLITE
      //    NEOVIEW
      //    REMEDY-AR-SYSTEM
      //    IMPALASIMBA
      //    MONDRIAN
      //    SYBASEIQ
      //    REDSHIFT
      //    NETEZZA
      //    VERTICA5
      //    FIREBIRD
      //    IMPALA
    }
  }
}
