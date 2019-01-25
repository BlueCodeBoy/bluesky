package com.bluesky.etl.steps.selectvalues

import java.sql.{Time, Timestamp}
import java.util.Date

import com.bluesky.etl.common.CommonSparkUtils
import com.bluesky.etl.steps.selectvalues.SelectValuesObject.SelectMetadataChangeSpark
import com.bluesky.etl.utils.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.pentaho.di.core.row.ValueMetaInterface
import org.pentaho.di.core.row.value.{ValueMetaBase, ValueMetaFactory}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


object SelectValuesObject extends Logging{

    case class SelectFieldSpark(val name:String,val rename:String,val length:Int,val precision:Int)
    case class SelectValuesObjectMeta(
                                       val select:Boolean ,val selectFields: Array[SelectFieldSpark],
                                       val deselect:Boolean ,val deleteFields: Array[String],
                                       val metadata:Boolean,val selectMetadataChangeSparks: Array[SelectMetadataChangeSpark]
                                     ){
    }

    def checkData(objectMeta: SelectValuesObjectMeta, dataFrame: DataFrame, sparkSession: SparkSession, stepName:String,stepId:String): DataFrame = {
      var dataFrameChange = dataFrame
      if(objectMeta.select){
        for(field<-objectMeta.selectFields){
          if(null!=field.rename && !field.rename.isEmpty &&
            field.name!=field.rename){
            dataFrameChange = dataFrameChange.withColumnRenamed(field.name,field.rename)
          }
        }
      }
      if(objectMeta.deselect){
        for(delete<-objectMeta.deleteFields){
          dataFrameChange = dataFrameChange.drop(delete)
        }
      }
      if(objectMeta.metadata){

          var change: SelectMetadataChangeSpark = null
          var toValueMeta: ValueMetaInterface = null
          var fromValueMeta: ValueMetaInterface = null
          var name: String = null
        var toValueMetaBases:mutable.Map[String,ValueMetaInterface] =  Map.empty
        val fromValueMetaBases:mutable.Map[String,ValueMetaInterface] = Map.empty
        dataFrameChange.schema.fields.map(data => {
          val st = CommonSparkUtils.convertStructFieldToValueMeta(data)
          val st1 = CommonSparkUtils.convertStructFieldToValueMeta(data)
          toValueMetaBases.put(data.name,st )
          fromValueMetaBases.put(data.name,st1 )
        })

//        if(fromValueMetaBases.isEmpty ||toValueMetaBases.isEmpty) {
          for (i <- 0 until objectMeta.selectMetadataChangeSparks.length) {
            change = objectMeta.selectMetadataChangeSparks.apply(i)
            name = change.name
            toValueMeta = toValueMetaBases.get(name).get
            if (null != change.rename && !change.rename.isEmpty &&
              change.rename != change.name) {
//              dataFrameChange = dataFrameChange.withColumnRenamed(change.name, change.rename)
              name = change.rename
            }
            if (null != change.conversionMask && !change.conversionMask.isEmpty) {
              toValueMeta.setConversionMask(change.conversionMask)
            }
            if (toValueMetaBases.getOrElse(name, null) == null) {
              toValueMetaBases.put(name, toValueMetaBases.get(change.name).get)
            }
            toValueMeta.setName(name)
            if (null != change.typeIndex && change.typeIndex != 0) {
//              toValueMeta = CommonSparkUtils.getValueMetaByID(change.typeIndex,name)
              toValueMeta = ValueMetaFactory.cloneValueMeta(toValueMeta, change.typeIndex)
              toValueMetaBases.remove(name)
              toValueMetaBases.put(name,toValueMeta)
              toValueMeta = toValueMetaBases.get(name).get
            } else {
              toValueMeta.setType(toValueMetaBases.get(name).get.getType)
            }
            toValueMeta.setLength(change.length)
            toValueMeta.setPrecision(change.precision)
            toValueMeta.setStorageType(change.storageType)
            toValueMeta.setDateFormatLenient(change.dateFormatLenient)
            toValueMeta.setDateFormatLocale(CommonSparkUtils.createLocale(change.dateFormatLocale))
            toValueMeta.setDateFormatTimeZone(CommonSparkUtils.createTimeZone(change.dateFormatTimeZone))
            toValueMeta.setLenientStringToNumber(change.lenientStringToNumber)
            if (null != change.encoding && !change.encoding.isEmpty) toValueMeta.setStringEncoding(change.encoding)
            if (null != change.decimalSymbol && !change.decimalSymbol.isEmpty) toValueMeta.setDecimalSymbol(change.decimalSymbol)
            if (null != change.groupingSymbol && !change.groupingSymbol.isEmpty) toValueMeta.setGroupingSymbol(change.groupingSymbol)
            if (null != change.currencySymbol && !change.currencySymbol.isEmpty) toValueMeta.setCurrencySymbol(change.currencySymbol)
          }
//        }
        var sp:SelectMetadataChangeSpark = null
        val metArray = objectMeta.selectMetadataChangeSparks.map(d=>{
          if(null!=d.rename && !d.rename.isEmpty &&
            d.rename!=d.name){
            d.rename
          }else{
            d.name
          }
        })
        val names = dataFrameChange.schema.fields.map(dat=>dat.name)
        var index = 0
        var data:Any = null
        val dataRow =  dataFrameChange.rdd.map(row=>{
          val array = row.toSeq.toArray
          for(name<-names){
            index = row.fieldIndex(name)
            fromValueMeta = fromValueMetaBases.get(name).get
            toValueMeta = toValueMetaBases.get(name).get
            sp = objectMeta.selectMetadataChangeSparks.apply(metArray.indexOf(name))
            if ( fromValueMeta.isStorageBinaryString()
              && sp.storageType == ValueMetaInterface.STORAGE_TYPE_NORMAL ) {
              array(index) = fromValueMeta.convertBinaryStringToNativeType( array(index).asInstanceOf[Array[Byte]])
            }
            if (sp.typeIndex != ValueMetaInterface.TYPE_NONE && fromValueMeta.getType() != toValueMeta.getType() ) {
              if(toValueMeta.getConversionMask!=null){
                fromValueMeta.setConversionMask(toValueMeta.getConversionMask)
              }
              try{
                data = toValueMeta.convertData(fromValueMeta,array(index))
              }catch {
                case e:Exception=>{
                  logError(e.getMessage,e)
                  data = toValueMeta.convertData(fromValueMeta,array(index))
                  throw new RuntimeException(e.getMessage)
                }
              }
              if(data.isInstanceOf[java.util.Date]){
                data =  new Timestamp(data.asInstanceOf[Date].getTime)
              }
              array(index) = data
            }
          }
          Row.fromSeq(array)
        })

        var dtype = new ArrayBuffer[StructField]()
        dataFrameChange.schema.foreach(datatype => {
          dtype += CommonSparkUtils.convertValueTypeToStrucType(toValueMetaBases.get(datatype.name).get)
        })
        dataFrameChange = sparkSession.createDataFrame(dataRow,StructType(dtype))
      }
      dataFrameChange
    }




  case  class SelectMetadataChangeSpark(
                                         // @Injection(name = "META_NAME", group = "METAS")
                                         val name:String,
                                         //  @Injection(name = "META_RENAME", group = "METAS")
                                         val rename:String,
                                         //  /** Meta: new Value type for this field or TYPE_NONE if no change needed! */
                                         val typeIndex:Int,
                                         //  /** Meta: new length of field */
                                         val length:Int,
                                         //@Injection(name = "META_PRECISION", group = "METAS")
                                         val precision:Int,
                                         // /** Meta: the storage type, NORMAL or BINARY_STRING */
                                         val storageType:Int,
                                         // @Injection(name = "META_CONVERSION_MASK", group = "METAS")
                                         val conversionMask:String,
                                         // @Injection(name = "META_DATE_FORMAT_LENIENT", group = "METAS")
                                         val dateFormatLenient:Boolean,
                                         // @Injection(name = "META_DATE_FORMAT_LOCALE", group = "METAS")
                                         val dateFormatLocale:String,
                                         //  @Injection(name = "META_DATE_FORMAT_TIMEZONE", group = "METAS")
                                         val dateFormatTimeZone:String,
                                         // META-DATA mode
                                         //@Injection(name = "META_LENIENT_STRING_TO_NUMBER", group = "METAS")
                                         val  lenientStringToNumber: Boolean,
                                         // META-DATA mode
                                         // @Injection(name = "META_DECIMAL", group = "METAS")
                                         val decimalSymbol: String,
                                         /** The grouping symbol for number conversions */
                                         //@Injection(name = "META_GROUPING", group = "METAS")
                                         val groupingSymbol: String,
                                         /** The currency symbol for number conversions */
                                         //@Injection(name = "META_CURRENCY", group = "METAS")
                                         val currencySymbol: String,
                                         /** The encoding to use when decoding binary data to Strings */
                                         //@Injection(name = "META_ENCODING", group = "METAS")
                                         val encoding: String
                                       )
}
