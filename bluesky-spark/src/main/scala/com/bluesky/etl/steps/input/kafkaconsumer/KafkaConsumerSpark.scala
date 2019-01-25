package com.bluesky.etl.steps.input.kafkaconsumer

import com.bluesky.etl.common.CommonSparkUtils
import com.bluesky.etl.core.enums.ConstanceType
import com.bluesky.etl.core.trans.{BaseMeta, BaseStep, SparkStepInterface, Trans}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.pentaho.di.core.row.RowMeta
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.consumer.{KafkaConsumerData, KafkaConsumerMeta}
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
class KafkaConsumerSpark(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
                         trans: Trans, stepDataInterface: StepDataInterface, baseMeta: BaseMeta) extends BaseStep(sparkSession: SparkSession,transMeta: TransMeta, stepMeta: StepMeta, copyNr: Int,
  trans: Trans, stepDataInterface: StepDataInterface, baseMeta: BaseMeta) with SparkStepInterface {
  /**
    *
    * @param data
    */
  override def restoreData(data: Dataset[Row]): Unit = {
    putDataToInputRowSet(data)
     restoreObject.synchronized{      restoreObject.wait()    }
  }
  var kafkaConsumerMeta: KafkaConsumerMeta = null
  var kafkaConsumerData: KafkaConsumerData = null

  /**
    *
    * @param sparkSession
    * @param smi
    * @param sdi
    * @return
    */
  override def processData(sparkSession: SparkSession, smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
    kafkaConsumerMeta = smi.asInstanceOf[KafkaConsumerMeta]
    kafkaConsumerData = sdi.asInstanceOf[KafkaConsumerData]


    val kafkaConsumerMetaObject = new KafkaConsumerMetaObject(kafkaConsumerMeta.getPollMS.toString.toInt,
      kafkaConsumerMeta.getMap.asScala,
      kafkaConsumerMeta.getKafkaProperties.asScala,
      kafkaConsumerMeta.getTopic,
      KafkaConsumerMeta.MIDDLE,KafkaConsumerMeta.CDC_FLAG,
      KafkaConsumerMeta.CDC_TIMESTAMP
    )
    trans.topicName = kafkaConsumerMetaObject.topic
    val streamcontext = new StreamingContext(sparkSession.sparkContext, Milliseconds(kafkaConsumerMetaObject.milliseconds))
    //缓存的数据
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> kafkaConsumerMetaObject.kafkaPro.getOrElse("bootstrap.servers",""),
//      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> kafkaConsumerMetaObject.kafkaPro.getOrElse("group.id","default"),
      "auto.offset.reset" -> kafkaConsumerMetaObject.kafkaPro.getOrElse("auto.offset.reset","smallest"))
    val km = new KafkaManager(kafkaParams)
    val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](streamcontext, kafkaParams, Set(kafkaConsumerMetaObject.topic))
    var offsetRanges = Array[OffsetRange]()
    //创建kafkastream同时获取偏移量
    val kafkadata = kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    var dataFrame:DataFrame = null
    val structType = StructType(CommonSparkUtils.mapStructAndKettle(kafkaConsumerMetaObject,kafkaConsumerMetaObject.fieldsMap.toMap))

    kafkadata.foreachRDD((rdd, time) => {
//      val  streamTime = time.asInstanceOf[org.apache.spark.streaming.Time].milliseconds
      if(!trans.stopped_) {
        if (!rdd.isEmpty()) {
          val tmpDataframe = sparkSession.createDataFrame(rdd.map(KafkaConsumerObject.loadMapData(kafkaConsumerMetaObject, _, time)), structType)
          if (!tmpDataframe.rdd.isEmpty())
            putData(kafkaConsumerData.outputRowMeta, tmpDataframe)
          trans.realTimeHistoryRowSet.putRow(null, tmpDataframe)
          //      if(Try(tmpDataframe.first()).isSuccess){
          //        if(null == dataFrame){
          //          dataFrame = tmpDataframe
          //        }else{
          //          dataFrame = dataFrame.union(tmpDataframe)
          //        }
          //      }
          km.updateZKOffsets(rdd)
        }else{
          time == null
        }
      }else{
        logInfo("trance is stopping")
        streamcontext.stop()
        false
      }

    })
//    if(null!=dataFrame && Try(dataFrame.first()).isSuccess){
//    }
    streamcontext.start()
    streamcontext.awaitTermination()
    trans.streamObject.synchronized{
      trans.streamObject.notifyAll()
    }
    setStopped(true)
    false
  }


}
