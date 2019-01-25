package com.bluesky.etl.common

import java.sql.{Connection, PreparedStatement}
import java.util.{Locale, StringTokenizer, TimeZone}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bluesky.etl.common.jdbcdialect._
import com.bluesky.etl.core.enums.{CommonFieldType, ConstanceType}
import com.bluesky.etl.steps.input.kafkaconsumer.KafkaConsumerMetaObject
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}

import collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.pentaho.di.core.Const
import org.pentaho.di.core.row.ValueMetaInterface
import org.pentaho.di.core.row.value._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by root on 18-8-17.
  */
case object CommonSparkUtils   {
  def filterNulldata(dataFrame: DataFrame,sparkSession: SparkSession):DataFrame={
//    var fdata:DataFrame = null
    var array:ArrayBuffer[Any] = null
    var data:Any = null
    sparkSession.createDataFrame(dataFrame.rdd.map(row=>{
      array = new ArrayBuffer[Any]()
      for (fieldName <- row.schema.fieldNames) {
        data = row.getAs(fieldName)
        if(null==data || "null".equals(data)||"".equals(data)){
          array+=null
        }else{
          array+=data
        }
      }
      Row.fromSeq(array)
    }), StructType(dataFrame.schema))
  }

    /**
      * 对SF_RES和SF_COL处理
      * @param s row的seq类型
      * @param row dataframe的row
      * @param map 本行数据校验的结果，例如{(column_1 -> true), (column_2) -> false, …… (column_n -> true)}
      * @param bRet 本行数据是否是问题数据，只要有一个字段的校验不通过都是false，true - 通过 false-不通过
      * @param stepName 校验节点名称
      * @return row的seq类型
      */
    def createResColumn(s:ListBuffer[Any], row:Row, map:mutable.Map[String,Boolean], bRet:Boolean, stepName:String,stepId:String) = {

        //生成本节点的问题结果
        val sf_col = s(row.fieldIndex(CommonFieldType.DATAFRAME_POSTFIX.toString))
        var jsonObject: JSONObject = null
        if (null == sf_col || "" == sf_col) {
            jsonObject = new JSONObject
        } else {
            jsonObject = JSON.parseObject(sf_col.toString)
        }


        val objMap = jsonObject.get(stepId).asInstanceOf[java.util.Map[String,Boolean]]
        if(null == objMap){
            jsonObject.put(stepId, mapAsJavaMap(map))
        } else {
            objMap.putAll(mapAsJavaMap(map))
            jsonObject.put(stepId, objMap)

        }

        s(row.fieldIndex(CommonFieldType.DATAFRAME_POSTFIX.toString)) = jsonObject.toString

        if (!bRet && s(row.fieldIndex(CommonFieldType.SF_RES.toString)) != CommonFieldType.SF_RES.id.toString) {
            s(row.fieldIndex(CommonFieldType.SF_RES.toString)) = CommonFieldType.SF_RES.id.toString.toInt
        }
        s
    }

  /**
    * Returns an available java.util.Locale object for the given localeCode.
    *
    * The localeCode code can be case insensitive, if it is available the method will find it and return it.
    *
    * @param localeCode
    * @return java.util.Locale.
    */
  def createLocale(localeCode: String): Locale = {
    var resultLocale: Locale = null
    if (!Const.isEmpty(localeCode)) {
      val parser = new StringTokenizer(localeCode, "_")
      if (parser.countTokens == 2) resultLocale = new Locale(parser.nextToken, parser.nextToken)
      else resultLocale = new Locale(localeCode)
    }
    resultLocale
  }

  def createTimeZone(timeZoneId: String): TimeZone = {
    var resultTimeZone: TimeZone = null
    if (!Const.isEmpty(timeZoneId)) return TimeZone.getTimeZone(timeZoneId)
    else resultTimeZone = TimeZone.getDefault
    resultTimeZone
  }
  val typeCodes = Array[String]("-", "Number", "String", "Date", "Boolean", "Integer", "BigNumber", "Serializable", "Binary", "Timestamp", "Internet Address")

  /**
    * convert spark to kettle
    * @param structField
    * @return
    */
  def convertStructFieldToValueMeta(structField: StructField): ValueMetaInterface={
    val vm = structField.dataType.getClass.getSimpleName.replaceAll("\\$","") match {
      case "ByteType"=>
        new ValueMetaString(structField.name)
//    ,typeCodes.indexOf("String"))
      case "ShortType"=>
        new ValueMetaBase(structField.name,typeCodes.indexOf("String"))
      case "IntegerType"=>
        new ValueMetaInteger(structField.name)
//    ,typeCodes.indexOf("Integer"))
      case "LongType"=>
        new ValueMetaBase(structField.name,typeCodes.indexOf("Number"))
      case "FloatType"=>
        new ValueMetaBase(structField.name,typeCodes.indexOf("BigNumber"))
      case "DoubleType"=>
        new ValueMetaBase(structField.name,typeCodes.indexOf("BigNumber"))
      case "DecimalType"=>
        new ValueMetaBase(structField.name,typeCodes.indexOf("BigNumber"))
      case "StringType"=>
        new ValueMetaString(structField.name)
//    ,typeCodes.indexOf("String"))
      case "BinaryType"=>
        new ValueMetaBase(structField.name,typeCodes.indexOf("Binary"))
      case "BooleanType"=>
        new ValueMetaBoolean(structField.name)
//    ,typeCodes.indexOf("Boolean"))
      case "TimestampType"=>
        new ValueMetaTimestamp(structField.name)
      case "DateType"=>
//        new ValueMetaBase(structField.name,typeCodes.indexOf("Date"))
        new ValueMetaDate(structField.name)
      case _ =>
        throw new RuntimeException( structField.dataType.toString +" not match")
    }
    vm.setOrigin("test")
    vm
  }

  /**
    *
    * @param index
    * @param name
    * @return
    */
  def getValueMetaByID(index:Int,name:String): ValueMetaInterface={
    val vm = typeCodes.apply(index) match {
      case "String"=>
        new ValueMetaString(name)
      case "Integer"=>
        new ValueMetaInteger(name)
      //    ,typeCodes.indexOf(""))
      case "Number"=>
        new ValueMetaBase(name)
//      case "BigNumber"=>
//        new ValueMetaBase(name,typeCodes.indexOf("BigNumber"))
//      case "BigNumber"=>
//        new ValueMetaBase(name,typeCodes.indexOf("BigNumber"))
      case "BigNumber"=>
        new ValueMetaBase(name,typeCodes.indexOf("BigNumber"))
//      case "String"=>
//        new ValueMetaString(name)
      //    ,typeCodes.indexOf("String"))
      case "Binary"=>
        new ValueMetaBase(name,typeCodes.indexOf("Binary"))
      case "Boolean"=>
        new ValueMetaBoolean(name)
      //    ,typeCodes.indexOf("Boolean"))
      case "Timestamp"=>
        new ValueMetaTimestamp(name)
      case "Date"=>
        //        new ValueMetaBase(structField.name,typeCodes.indexOf("Date"))
        new ValueMetaDate(name)
      case _ =>
        throw new RuntimeException( name +" not match")
    }
    vm.setOrigin("test")
    vm
  }

  /**
    * convert kettle to spark
    * @param valueMetaInterface
    * @return
    */
  def convertValueTypeToStrucType(valueMetaInterface: ValueMetaInterface):StructField= {
    typeCodes.apply(valueMetaInterface.getType) match {
      case "-" =>
        new StructField(valueMetaInterface.getName, StringType, true)
      case "Number" =>
        new StructField(valueMetaInterface.getName, DecimalType.SYSTEM_DEFAULT, true)
      case "String" =>
        new StructField(valueMetaInterface.getName, StringType, true)
      case "Date" =>
//        new StructField(valueMetaInterface.getName, DateType, true)
        new StructField(valueMetaInterface.getName, TimestampType, true)
      case "Boolean" =>
        new StructField(valueMetaInterface.getName, BooleanType, true)
      case "Integer" =>
        new StructField(valueMetaInterface.getName, IntegerType, true)
      case "BigNumber" =>
        new StructField(valueMetaInterface.getName, DecimalType.SYSTEM_DEFAULT, true)
      case "Serializable" =>
        new StructField(valueMetaInterface.getName, StringType, true)
      case "Binary" =>
        new StructField(valueMetaInterface.getName, BinaryType, true)
      case "Timestamp" =>
        new StructField(valueMetaInterface.getName, TimestampType, true)
      case "Internet Address" =>
        new StructField(valueMetaInterface.getName, StringType, true)
      case _ =>
        throw new RuntimeException(typeCodes.apply(valueMetaInterface.getType)+" not match")



    }
  }

  /**
    *
    * @param key
    * @param dataType
    * @return
    */
  def caseType(key:String,dataType:Int,metadata: Metadata):StructField={
    dataType  match {
      case 0 =>
        //          val TYPE_NONE: Int = 0
        if(null!=metadata){
          StructField(key, StringType, true,metadata)
        }else{
          StructField(key, StringType, true)
        }
      case 1 =>
        //          val TYPE_NUMBER: Int = 1
        if(null!=metadata) {
          StructField(key, DoubleType, true,metadata)
        }else{
          StructField(key, DoubleType, true)
        }
      case 2 =>
        //          val TYPE_STRING: Int = 2
        if(null!=metadata) {
          StructField(key, StringType, true,metadata)
        }else{
          StructField(key, StringType, true)
        }
      case 3 =>
        //          val TYPE_DATE: Int = 3
        if(null!=metadata) {
          StructField(key, DateType, true,metadata)
        }else{
          StructField(key, DateType, true)
        }
      case 4 =>
        //          val TYPE_BOOLEAN: Int = 4
        if(null!=metadata){
          StructField(key, BooleanType, true,metadata)
        }else{
          StructField(key, BooleanType, true)
        }
      case 5 =>
        //          val TYPE_INTEGER: Int = 5
        if(null!=metadata) {
          StructField(key, IntegerType, true,metadata)
        }else{
          StructField(key, IntegerType, true)
        }
      case 6 =>
        //          val TYPE_BIGNUMBER: Int = 6
        if(null!=metadata) {
          StructField(key, DecimalType(20,4), true,metadata)
        }else{
          StructField(key, DecimalType(20,4), true)
        }
      case 7 =>
      //          val TYPE_SERIALIZABLE: Int = 7
      //          arrayStructField += StructField(key,ArrayType , true)
        null
      case 8 =>
        //          val TYPE_BINARY: Int = 8
        if(null!=metadata) {
          StructField(key, ByteType, true,metadata)
        }else{
          StructField(key, ByteType, true)
        }
      case 9 =>
        //          val TYPE_TIMESTAMP: Int = 9
        if(null!=metadata) {
          StructField(key, TimestampType, true,metadata)
        }else{
          StructField(key, TimestampType, true)
        }
      case 10 =>
      //          val TYPE_INET: Int = 10
      //          arrayStructField += StructField(key, , false)
        null
    }
  }

  /**
    *
    * @param kafkaConsumerMetaObject
    * @param mapData
    * @return
    */
  def mapStructAndKettle(kafkaConsumerMetaObject: KafkaConsumerMetaObject,mapData:scala.collection.immutable.Map[String,Integer]):ArrayBuffer[StructField]={
    val arrayStructField = new ArrayBuffer[StructField]()
    for(key<-mapData.keySet){
      arrayStructField+=caseType(key,mapData.getOrElse(key,0).toString.toInt,null)
    }
    arrayStructField += StructField(kafkaConsumerMetaObject.cdcFlag, IntegerType, true)
    arrayStructField += StructField(kafkaConsumerMetaObject.cdcTimeStamp, TimestampType, true)
    arrayStructField += StructField(ConstanceType.OGG_OPTS_TIMESTAMP.toString, LongType, true)
    arrayStructField
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
