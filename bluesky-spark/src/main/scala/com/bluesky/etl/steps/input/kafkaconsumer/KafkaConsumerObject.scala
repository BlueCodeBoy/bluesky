package com.bluesky.etl.steps.input.kafkaconsumer

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bluesky.etl.common.CommonSparkUtils
import com.bluesky.etl.core.enums.CommonFieldType
import java.util.Calendar

import com.bluesky.etl.core.trans.BaseMeta
import com.bluesky.etl.utils.Logging
import kafka.serializer.StringDecoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.pentaho.di.core.database.DatabaseMeta
import org.pentaho.di.core.row.ValueMetaInterface

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.runtime.Nothing$

case class KafkaConsumerMetaObject(milliseconds:Long,
                              fieldsMap:mutable.Map[String,Integer],
                              kafkaPro:mutable.Map[String,String],
                              topic:String,
                              middle:String,
                                cdcFlag:String,
                                   cdcTimeStamp:String
                          ) extends BaseMeta with Serializable{


}
/**
  * Created by root on 18-8-16.
  */
case object KafkaConsumerObject extends Logging{
    final val canDo:String = "CAN_DO"

  var mapData:immutable.Map[String,Integer] = null
  val s1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  def loadMapData(kafkaConsumerMetaObject: KafkaConsumerMetaObject,oringinalData:(String,String),time:Time
):Row={
    if(null == mapData){
      mapData = kafkaConsumerMetaObject.fieldsMap.toMap
    }
    var arrayBuffer = new ArrayBuffer[Any](mapData.keys.size+3)
    try{
      val jsonObject = JSON.parseObject(oringinalData._2.toString)
      var tmpArray:List[String] = null
      var oggOpts:Long = 0
      var tmpJSONObject = new JSONObject()
      for (key <- mapData.keySet) {
        if(key.contains(kafkaConsumerMetaObject.middle)){
          tmpArray = key.split(kafkaConsumerMetaObject.middle).toList
          if(tmpArray.length==2){
            tmpJSONObject = jsonObject.get(tmpArray.apply(0)).asInstanceOf[JSONObject]
            if(null == tmpJSONObject){
              arrayBuffer+= null
            }else{
              if(tmpJSONObject.containsKey(tmpArray.apply(1))){
                if(null != tmpJSONObject.get(tmpArray.apply(1)) ){
                  if( null!=mapData.getOrElse(key,null)) {
                    CommonSparkUtils.typeCodes.apply(mapData.getOrElse(key,3).toString.toInt) match {
                      case "String"=>
                        arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1)).toString
                      case "Integer"=>
                        if(tmpJSONObject.get(tmpArray.apply(1)).isInstanceOf[Long]) {
                          arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1)).toString.toLong.toInt
                        }else if(tmpJSONObject.get(tmpArray.apply(1)).isInstanceOf[Float]){
                          arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1)).toString.toFloat.toInt
                        }else if(tmpJSONObject.get(tmpArray.apply(1)).isInstanceOf[java.math.BigDecimal]){
                          arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1)).asInstanceOf[java.math.BigDecimal].intValue()
                        }else{
                          arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1)).toString.toInt
                        }
                      case "BigNumber"=>
                        if(tmpJSONObject.get(tmpArray.apply(1)).isInstanceOf[Integer]){
                          arrayBuffer+=new java.math.BigDecimal(tmpJSONObject.get(tmpArray.apply(1)).asInstanceOf[Integer])
                        }else{
                          arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1))
                        }

                    }
                  }else{
                    arrayBuffer+=tmpJSONObject.get(tmpArray.apply(1))
                  }
                }else{
                  arrayBuffer += ""
                }
              }else{
                arrayBuffer+= null
              }
            }
          }else{
            arrayBuffer+= null
          }
        }else{
//          if (key == "op_ts") {
//            var opstime = if (jsonObject.get(key) == null) "" else jsonObject.get("op_ts").toString
//            if (opstime.length > 23) {
//              opstime = opstime.substring(0, 23)
//            }
//            arrayBuffer+=opstime
//
//            val date = s1.parse(opstime)
//            val calendar = Calendar.getInstance
//            calendar.setTime(date)
//            calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8)
//            oggOpts = calendar.getTime.getTime
//          }else{
            if(jsonObject.get(key).isInstanceOf[JSONArray]){
              arrayBuffer+=jsonObject.get(key).toString
            }else{
              arrayBuffer+=jsonObject.get(key)
            }
//          }
        }
      }
      arrayBuffer+=1
      arrayBuffer+==new Timestamp(System.currentTimeMillis())
      arrayBuffer+=oggOpts
    }catch {
      case e:Exception=>
        e.printStackTrace()
        logError(e.getMessage,e)
    }
    if(arrayBuffer.isEmpty){
      arrayBuffer++=List.fill(mapData.keys.size+3)(null)
    }
    Row.fromSeq(arrayBuffer)

  }
}
