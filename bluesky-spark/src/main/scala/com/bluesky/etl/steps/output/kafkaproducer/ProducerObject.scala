package com.bluesky.etl.steps.output.kafkaproducer

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig

import scala.collection.mutable

/**
  * Created by root on 18-9-29.
  */
// singleton object
object ProducerObject extends Serializable{
  var producerPool:mutable.ListBuffer[Producer[String,String]] = mutable.ListBuffer.empty
  sys.addShutdownHook{
    for(a<-producerPool){
      a.close
    }
  }
  // factory method
  def apply( kafkaProperties:mutable.Map[String,String]): Producer[String,String] = {
    var producer:Producer[String,String] = null
    if(producerPool.isEmpty){
      val props = new Properties()
      props.put("metadata.broker.list", kafkaProperties.getOrElse("metadata.broker.list",""))
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      for( result <- Range(0, 5) ){
        val kafkaConfig = new ProducerConfig(props)
        producer = new Producer[String, String](kafkaConfig)
        producerPool+=producer
      }
    }else{
      producer = producerPool.apply((new java.util.Random).nextInt(producerPool.size))
    }
    producer
  }
}
