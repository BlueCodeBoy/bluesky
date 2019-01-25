package com.bluesky.etl.steps.output.kafkaproducer

import com.alibaba.fastjson.JSONObject
import com.bluesky.etl.common.CommonSparkUtils
import com.bluesky.etl.core.trans.BaseMeta
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

/**
  *
  */
class KafkaProducerMetaObject(   kafkaProperties:mutable.Map[String,String],
                                 topics:String,
                                 middles:String,
                                 islineDatas:Boolean,
                                 dataFormateTypes:String
                             ) extends BaseMeta with Serializable{
  val kafkaPro = kafkaProperties
  val topic = topics
  val middle = middles
  val islineData = islineDatas
  val dataFormateType = dataFormateTypes}
case object KafkaProducerObject {
  def processData(kafkaProducerMetaObject: KafkaProducerMetaObject,producer:Producer[String,String], row: Row): Unit ={
    if(kafkaProducerMetaObject.islineData){
      kafkaProducerMetaObject.dataFormateType match{
        case "JSON"=>{
          var jstring: JSONObject = new JSONObject()
          row.schema.fieldNames.foreach(d => {
            if(d.toString.indexOf(kafkaProducerMetaObject.middle)>0){
              val keys = d.toString.split(kafkaProducerMetaObject.middle)
              var jSONObject: JSONObject = jstring.get(keys.apply(0)).asInstanceOf[JSONObject]
              if(jSONObject==null){
                jSONObject = new JSONObject()
              }
              jSONObject.put(d.toString, row.getAs(d.toString))
              jstring.remove(keys.apply(0))
              jstring.put(keys.apply(0), jSONObject)
            }else{
              jstring.put(d.toString, row.getAs(d.toString))
            }
          })
          producer.send(new KeyedMessage[String, String](kafkaProducerMetaObject.topic, jstring.toString()))
        }
      }
    }

  }


}
