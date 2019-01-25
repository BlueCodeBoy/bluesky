package com.bluesky.etl.steps.output.kafkaproducer

import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.consumer.{KafkaConsumerData, KafkaConsumerMeta}
//import org.pentaho.di.trans.kafka.producer.{KafkaProducerData, KafkaProducerMeta}
import org.pentaho.di.trans.step.{StepDataInterface, StepMeta, StepMetaInterface}

import scala.collection.JavaConverters.{mapAsScalaMapConverter, propertiesAsScalaMapConverter}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
/**
  *
  * @param transMeta
  * @param stepMeta
  * @param copyNr
  * @param trans
  * @param stepDataInterface
  * @param baseMeta
  */
class KafkaProducerSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                         trans: Trans, stepDataInterface: StepDataInterface, baseMeta:BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
    trans: Trans, stepDataInterface: StepDataInterface,baseMeta:BaseMeta) with SparkStepInterface {
    /**
      *
      * @param data
      */
    override def restoreData(data: Dataset[Row]): Unit = {
        putDataToInputRowSet(data)
         restoreObject.synchronized{      restoreObject.wait()    }
    }
//    var kafkaProducerMeta:KafkaProducerMeta = null
//    var kafkaProducerData:KafkaProducerData = null

    /**
      *
      * @param sparkSession
      * @param smi
      * @param sdi
      * @return
      */
    override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
//        kafkaProducerMeta = smi.asInstanceOf[KafkaProducerMeta]
//        kafkaProducerData = sdi.asInstanceOf[KafkaProducerData]
//
//
//
//        val kafkaProducerMetaObject = new KafkaProducerMetaObject(
//            kafkaProducerMeta.getKafkaProperties.asScala,
//            kafkaProducerMeta.getTopic,
//            KafkaConsumerMeta.MIDDLE,
//            kafkaProducerMeta.isIslineData,
//            kafkaProducerMeta.getDataformate
//        )
//        val dataFrame: DataFrame = getData()
//        if (null == dataFrame) {
//            setOutputDone()
//            return false
//        }
//        if(null!=dataFrame){
//          if(Try(dataFrame.first()).isSuccess){
//            dataFrame.foreachPartition(patitiondata => {
//                val producer = ProducerObject(kafkaProducerMetaObject.kafkaPro)
//                patitiondata.foreach(KafkaProducerObject.processData(kafkaProducerMetaObject,producer, _))
//            })
//            if(!hasNextStep()){
//                moveData(dataFrame)
//            }
//          }
//
//        }
        true
    }
}
