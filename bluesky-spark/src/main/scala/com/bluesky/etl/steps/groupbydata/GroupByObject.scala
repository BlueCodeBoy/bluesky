package com.bluesky.etl.steps.groupbydata

import java.util
import java.util.List

import org.apache.commons.math.stat.descriptive.rank.Percentile
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

object GroupByObject {

    case class GroupByObjectMeta(aggregateField:collection.immutable.List[String],
                                 aggregateType:collection.immutable.List[String],
                                 subjectField:collection.immutable.List[String],
                                 values:collection.immutable.List[String],
                                             groupFields:collection.immutable.List[String],
                                 groupTypeCode:collection.immutable.List[String])


  /**
    *
    * @param objectMeta
    * @param dataFrame
    * @param sparkSession
    * @return
    */
    def groupData(objectMeta:GroupByObjectMeta ,dataFrame: DataFrame,sparkSession: SparkSession): DataFrame ={
       val oj = sparkSession.sparkContext.broadcast(objectMeta)
       val data = dataFrame.rdd.map(row=>{(objectMeta.groupFields.map(key=>row.getAs(key).toString).mkString("bluesky_group_middle"),(row,oj))})
        .combineByKey(createCombiner,mergeValue,mergeCombiners).map(r=>Row.fromSeq(r._2._1))
        sparkSession.createDataFrame(data,createSchema(objectMeta,dataFrame.first()))
    }

  /**
    *
    * @param objectMeta
    * @param row
    * @return
    */
  def createSchema(objectMeta: GroupByObjectMeta,row:Row):StructType={
    val dataLength = objectMeta.aggregateField.length
    val arrayBuffer = new ArrayBuffer[StructField](objectMeta.groupFields.length)
    var name:String = null
    for(index <- 0 until dataLength){
      name = objectMeta.aggregateField.apply(index)
        objectMeta.aggregateType.apply(index) match {
          case "-" =>
          case "SUM"=>
            row.schema(objectMeta.subjectField.apply(index)).dataType match {
              case ByteType|ShortType|IntegerType |LongType=>
                arrayBuffer.append(new StructField(name,LongType,true))
              case StringType=>
                arrayBuffer.append(new StructField(name,StringType,true))
              case FloatType |DoubleType=>
                arrayBuffer.append(new StructField(name,DoubleType,true))
              case DecimalType()=>
                arrayBuffer.append(new StructField(name,DecimalType(20,4),true))
            }
          case "AVERAGE"=>
            arrayBuffer.append(new StructField(name,DoubleType,true))
          case "PERCENTILE" |"MEDIAN"=>
            arrayBuffer.append(new StructField(name,DoubleType,true))
          case "MIN"|"MAX" |"FIRST"|"LAST"|"FIRST_INCL_NULL"|"LAST_INCL_NULL"=>
            row.schema(objectMeta.subjectField.apply(index)).dataType match {
              case ByteType=>
                arrayBuffer.append(new StructField(name,ByteType,true))
              case ShortType=>
                arrayBuffer.append(new StructField(name,ShortType,true))
              case IntegerType=>
                arrayBuffer.append(new StructField(name,IntegerType,true))
              case LongType=>
                arrayBuffer.append(new StructField(name,LongType,true))
              case FloatType=>
                arrayBuffer.append(new StructField(name,FloatType,true))
              case DoubleType=>
                arrayBuffer.append(new StructField(name,DoubleType,true))
              case StringType=>
                arrayBuffer.append(new StructField(name,StringType,true))
              case DecimalType()=>
                arrayBuffer.append(new StructField(objectMeta.aggregateField.apply(index),DecimalType(20,4),true))
            }
          case "COUNT_ALL"=>
            arrayBuffer.append(new StructField(name,LongType,true))
          case "CONCAT_COMMA"=>
            arrayBuffer.append(new StructField(name,StringType,true))
          case "STD_DEV"=>
            arrayBuffer.append(new StructField(name,DoubleType,true))
          case "CONCAT_STRING"=>
            arrayBuffer.append(new StructField(name,StringType,true))
          case "COUNT_DISTINCT"=>
            arrayBuffer.append(new StructField(name,LongType,true))
          case  "COUNT_ANY"=>
            arrayBuffer.append(new StructField(name,LongType,true))
        }
    }
    StructType(arrayBuffer)
  }

  /**
    *
    * @param data
    * @return
    */
  def createCombiner(data:(Row,Broadcast[GroupByObjectMeta])):(Array[Any],Broadcast[GroupByObjectMeta])={
     val objectMeta = data._2.value
     val arrayData = new Array[Any](objectMeta.values.length)
     val dataLength = objectMeta.values.length
     for(index <- 0 until dataLength){
       if(null == objectMeta.values.apply(index)){
         objectMeta.aggregateType.apply(index) match {
           case "-" =>
           case "SUM"=>
             caslataByRow(data._1,objectMeta,index,arrayData,null,objectMeta.aggregateType.apply(index))
           case "AVERAGE"=>
             caslataByRow(data._1,objectMeta,index,arrayData,null,objectMeta.aggregateType.apply(index))
           case "PERCENTILE" |"MEDIAN"=>
             caslataByRow(data._1,objectMeta,index,arrayData,null,objectMeta.aggregateType.apply(index))
           case "MIN"=>
             caslataByRow(data._1,objectMeta,index,arrayData,null,objectMeta.aggregateType.apply(index))
           case "MAX"=>
             caslataByRow(data._1,objectMeta,index,arrayData,null,objectMeta.aggregateType.apply(index))
           case "COUNT_ALL"=>
             if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
               arrayData(index) = 1L
             }
           case "CONCAT_COMMA"=>
             if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
               arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
             }
           case "FIRST"=>
             if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
               arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
             }
           case "LAST"=>
             if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
               arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
             }
           case "FIRST_INCL_NULL"=>
             arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index))
           case "LAST_INCL_NULL"=>
             arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index))
           case "STD_DEV"=>
             val n = 1
             val x = data._1.getDouble(data._1.fieldIndex(objectMeta.subjectField.apply(index)))
             // for standard deviation null is exact 0
             var sum = if (x == null) 0.0 else x
             var mean = 0.0
             val delta = x - mean
             mean = mean + (delta / n)
             sum = sum + delta * (x - mean)
             arrayData(index) = (mean,sum,n)
           case "CONCAT_STRING"=>
             if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
               arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
             }
           case "COUNT_DISTINCT"=>
             if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
               val rData =  new mutable.HashSet[Any]()
               rData.add(data._1.getAs(objectMeta.subjectField.apply(index)).toString)
               arrayData(index) =rData
             }
           case  "COUNT_ANY"=>
             arrayData(index) = 1L

         }
       }else{
         arrayData(index) = objectMeta.values.apply(index)
       }
     }
    (arrayData,data._2)
  }

  /**
    *
    * @param od
    * @param data
    * @return
    */
  def mergeValue(od:(Array[Any],Broadcast[GroupByObjectMeta]),data:(Row,Broadcast[GroupByObjectMeta])):(Array[Any],Broadcast[GroupByObjectMeta])={
    val objectMeta = data._2.value
    val arrayData = new Array[Any](objectMeta.values.length)
    val dataLength = objectMeta.values.length
    val originalArray = od._1
    for(index <- 0 until dataLength){
      if(null == objectMeta.values.apply(index)){
        objectMeta.aggregateType.apply(index) match {
          case "-" =>
          case "SUM"=>
            caslataByRow(data._1,objectMeta,index,arrayData,originalArray,objectMeta.aggregateType.apply(index))
          case "AVERAGE"=>
            caslataByRow(data._1,objectMeta,index,arrayData,originalArray,objectMeta.aggregateType.apply(index))
          case "PERCENTILE" |"MEDIAN"=>
            caslataByRow(data._1,objectMeta,index,arrayData,originalArray,objectMeta.aggregateType.apply(index))
          case "MIN"=>
            caslataByRow(data._1,objectMeta,index,arrayData,originalArray,objectMeta.aggregateType.apply(index))
          case "MAX"=>
            caslataByRow(data._1,objectMeta,index,arrayData,originalArray,objectMeta.aggregateType.apply(index))
          case "COUNT_ALL"=>
            if(null == data._1.getAs(objectMeta.subjectField.apply(index))){
              arrayData(index) = originalArray(index).asInstanceOf[Long]+1L
            }
          case "CONCAT_COMMA"=>
            if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
              if(null == originalArray(index)){
                arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
              }else{
                arrayData(index) =  s"${originalArray(index)},${data._1.getAs(objectMeta.subjectField.apply(index)).toString}"
              }
            }
          case "FIRST"=>
            if(null != data._1.getAs(objectMeta.subjectField.apply(index))&& originalArray(index)==null){
              arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
            }
          case "LAST"=>
            if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
              arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
            }
          case "FIRST_INCL_NULL"=>
            arrayData(index) =  od._1.apply(index)
          case "LAST_INCL_NULL"=>
            arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index))
          case "STD_DEV"=>
            val od = originalArray.apply(index).asInstanceOf[Tuple3[Double,Int,Int]]
            val n = od._3+1
            val x = data._1.getDouble(data._1.fieldIndex(objectMeta.subjectField.apply(index)))
            // for standard deviation null is exact 0
            var sum = if (x == null) 0.0 else x
            var mean = od._1
            val delta = x - mean
            mean = mean + (delta / n)
            sum = sum + delta * (x - mean)
            arrayData(index) = (mean,sum,n)
          case "CONCAT_STRING"=>
            if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
              if(null == originalArray(index)){
                arrayData(index) =  data._1.getAs(objectMeta.subjectField.apply(index)).toString
              }else{
                arrayData(index) =  s"${originalArray(index)}${data._1.getAs(objectMeta.subjectField.apply(index)).toString}"
              }
            }
          case "COUNT_DISTINCT"=>
            if(null != data._1.getAs(objectMeta.subjectField.apply(index))){
              val rData =  originalArray.apply(index).asInstanceOf[mutable.HashSet[Any]]
              rData.add(data._1.getAs(objectMeta.subjectField.apply(index)).toString)
              arrayData(index) =rData
            }
          case "COUNT_ANY"=>
            arrayData(index) = originalArray(index).asInstanceOf[Long]+1L
        }
      }else{
        arrayData(index) = objectMeta.values.apply(index)
      }
    }
    (arrayData,data._2)
  }

  /**
    *
    * @param a1
    * @param a2
    * @return
    */
  def mergeCombiners(a1:(Array[Any],Broadcast[GroupByObjectMeta]),a2:(Array[Any],Broadcast[GroupByObjectMeta])):(Array[Any],Broadcast[GroupByObjectMeta])={
    val arrayData = new Array[Any](a1._1.length)
    val objectMeta = a1._2.value
    val dataLength = objectMeta.values.length
    for(index <- 0 until dataLength){
      if(null == objectMeta.values.apply(index)){
        objectMeta.aggregateType.apply(index) match {
          case "-" =>
          case "SUM"=>
            resultRowForNumber(objectMeta,index,a1._1,a2._1,arrayData,objectMeta.aggregateType.apply(index))
          case "AVERAGE"=>
            resultRowForNumber(objectMeta,index,a1._1,a2._1,arrayData,objectMeta.aggregateType.apply(index))
          case "PERCENTILE" |"MEDIAN"=>
            resultRowForNumber(objectMeta,index,a1._1,a2._1,arrayData,objectMeta.aggregateType.apply(index))
          case "MIN"=>
            resultRowForNumber(objectMeta,index,a1._1,a2._1,arrayData,objectMeta.aggregateType.apply(index))
          case "MAX"=>
            resultRowForNumber(objectMeta,index,a1._1,a2._1,arrayData,objectMeta.aggregateType.apply(index))
          case "COUNT_ALL"=>
            arrayData(index) = a1._1.apply(index).asInstanceOf[Long]+a2._1.apply(index).asInstanceOf[Long]
          case "CONCAT_COMMA"=>
            arrayData(index) = s"${a1._1.apply(index).toString},${a2._1.apply(index).toString}"
          case "FIRST"=>
            arrayData(index) = a1._1.apply(index).toString
          case "LAST"=>
            arrayData(index) = a2._1.apply(index).toString
          case "FIRST_INCL_NULL"=>
            arrayData(index) = a1._1.apply(index).toString
          case "LAST_INCL_NULL"=>
            arrayData(index) = a2._1.apply(index).toString
          case "STD_DEV"=>
            val od = a1._1.apply(index).asInstanceOf[Tuple3[Double,Int,Int]]
            val sum = od._2 /od._3
            val sd1 = Math.sqrt(sum).toDouble

            val od2 = a2._1.apply(index).asInstanceOf[Tuple3[Double,Int,Int]]
            val sum2 = od2._2 /od._3
            val sd2 = Math.sqrt(sum2).toDouble
            arrayData(index) = (sd1+sd2)/2
          case "CONCAT_STRING"=>
            arrayData(index) = s"${a1._1.apply(index).toString}${a2._1.apply(index).toString}"
          case "COUNT_DISTINCT"=>
            val rData =  a1._1.apply(index).asInstanceOf[mutable.HashSet[Any]] ++ a2._1.apply(index).asInstanceOf[mutable.HashSet[Any]]
            arrayData(index) = rData.size
          case "COUNT_ANY"=>
            arrayData(index) = a1._1.apply(index).asInstanceOf[Long]+a2._1.apply(index).asInstanceOf[Long]
        }
      }else{
        arrayData(index) = objectMeta.values.apply(index)
      }
    }
    (arrayData,a1._2)

  }

  /**
    *
    * @param objectMeta
    * @param index
    * @param arrayData1
    * @param arrayData2
    * @param arrayData
    * @param typeCode
    */
  def resultRowForNumber(objectMeta:GroupByObjectMeta,index:Int,arrayData1:Array[Any],arrayData2:Array[Any] , arrayData:Array[Any]  , typeCode:String):Unit={
    var data1 = arrayData1(index)
    var data2 = arrayData2(index)
    var avgdata1:Tuple2[Any,Long]  = null
    var avgdata2:Tuple2[Any,Long]  = null
    typeCode match {
      case "SUM"=>
        if(data1.isInstanceOf[java.math.BigDecimal]){
          arrayData(index) = data1.asInstanceOf[java.math.BigDecimal].add(data2.asInstanceOf[java.math.BigDecimal])
        }else if(data1.isInstanceOf[Double]){
          arrayData(index) = data1.asInstanceOf[Double] + data2.asInstanceOf[Double]
        }else if(data1.isInstanceOf[Float]){
          arrayData(index) = (data1.asInstanceOf[Float] + data2.asInstanceOf[Float]).toDouble
        }else if(data1.isInstanceOf[Byte]){
          arrayData(index) = (data1.asInstanceOf[Byte] + data2.asInstanceOf[Byte]).toLong
        }else if(data1.isInstanceOf[Short]){
          arrayData(index) = (data1.asInstanceOf[Short] + data2.asInstanceOf[Short]).toLong
        }else if(data1.isInstanceOf[Int]){
          arrayData(index) = (data1.asInstanceOf[Int] + data2.asInstanceOf[Int]).toLong
        }else if(data1.isInstanceOf[Long]) {
          arrayData(index) = data1.asInstanceOf[Long] + data2.asInstanceOf[Long]
        }
      case "AVERAGE"=>
        avgdata1 = data1.asInstanceOf[Tuple2[Any,Long]]
        avgdata2 = data2.asInstanceOf[Tuple2[Any,Long]]

        if(avgdata1._1.isInstanceOf[java.math.BigDecimal]){
          arrayData(index) = (avgdata1._1.asInstanceOf[java.math.BigDecimal].add(avgdata2._1.asInstanceOf[java.math.BigDecimal])).divide(new java.math.BigDecimal(avgdata1._2+ avgdata2._2)).doubleValue()
        }else if(avgdata1._1.isInstanceOf[Double]){
          arrayData(index) = (avgdata1._1.asInstanceOf[Double] + data2.asInstanceOf[Double])/(avgdata1._2+avgdata2._2)
        }else if(avgdata1._1.isInstanceOf[Float]){
          arrayData(index) = (avgdata1._1.asInstanceOf[Float] + data2.asInstanceOf[Float]).toDouble/(avgdata1._2+avgdata2._2)
        }else if(avgdata1._1.isInstanceOf[Byte]){
          arrayData(index) = (avgdata1._1.asInstanceOf[Byte] + data2.asInstanceOf[Byte]).toLong/(avgdata1._2+avgdata2._2)
        }else if(avgdata1._1.isInstanceOf[Short]){
          arrayData(index) = (avgdata1._1.asInstanceOf[Short] + data2.asInstanceOf[Short]).toLong/(avgdata1._2+avgdata2._2)
        }else if(avgdata1._1.isInstanceOf[Int]){
          arrayData(index) = (avgdata1._1.asInstanceOf[Int] + data2.asInstanceOf[Int]).toLong/(avgdata1._2+avgdata2._2)
        }else if(avgdata1._1.isInstanceOf[Long]) {
          arrayData(index) = avgdata1._1.asInstanceOf[Long] + data2.asInstanceOf[Long]/(avgdata1._2+avgdata2._2)
        }
      case "MEDIAN" | "PERCENTILE"=>
        val percentile: Double = 50.0
        val valuesList: ListBuffer[Double] = data1.asInstanceOf[ListBuffer[Double]] ++ data2.asInstanceOf[ListBuffer[Double]]
        val values: Array[Double] = new Array[Double](valuesList.size)
        var v: Int = 0
        while ( {
          v < values.length
        }) {
          values(v) = valuesList.apply(v)

          {
            v += 1; v - 1
          }
        }
        arrayData(index) = new Percentile().evaluate(values, percentile)

      case "MIN"=>
        if(data1.isInstanceOf[java.math.BigDecimal]){
          if(data2.asInstanceOf[java.math.BigDecimal].compareTo(data1.asInstanceOf[java.math.BigDecimal]) == -1){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Double]){
          if(data2.asInstanceOf[Double] < data1.asInstanceOf[Double]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Float]){
          if(data2.asInstanceOf[Float] < data1.asInstanceOf[Float]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Byte]){
          if(data2.asInstanceOf[Byte] < data1.asInstanceOf[Byte]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Short]){
          if(data2.asInstanceOf[Short] < data1.asInstanceOf[Short]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Int]){
          if(data2.asInstanceOf[Int] < data1.asInstanceOf[Int]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Long]) {
          if(data2.asInstanceOf[Long] < data1.asInstanceOf[Long]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }
      case "MAX"=>
        if(data1.isInstanceOf[java.math.BigDecimal]){
          if(data2.asInstanceOf[java.math.BigDecimal].compareTo(data1.asInstanceOf[java.math.BigDecimal]) == -1){
            arrayData(index) = data1
          }else{
            arrayData(index) = data2
          }
        }else if(data1.isInstanceOf[Double]){
          if(data2.asInstanceOf[Double] > data1.asInstanceOf[Double]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Float]){
          if(data2.asInstanceOf[Float] > data1.asInstanceOf[Float]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Byte]){
          if(data2.asInstanceOf[Byte] > data1.asInstanceOf[Byte]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Short]){
          if(data2.asInstanceOf[Short] > data1.asInstanceOf[Short]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Int]){
          if(data2.asInstanceOf[Int] > data1.asInstanceOf[Int]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }else if(data1.isInstanceOf[Long]) {
          if(data2.asInstanceOf[Long] > data1.asInstanceOf[Long]){
            arrayData(index) = data2
          }else{
            arrayData(index) = data1
          }
        }
    }


  }
  /**
    *
    * @param row
    * @param objectMeta
    * @param index
    * @param arrayData
    * @param originalArray
    * @param typeCode
    */
  def caslataByRow(row:Row,objectMeta:GroupByObjectMeta,index:Int,arrayData:Array[Any],originalArray:Array[Any] , typeCode:String):Unit={
    val isInit  = originalArray == null
    var avgdata:Tuple2[Any,Long]  = null
    var percent:ListBuffer[Double] = null
    row.schema(objectMeta.subjectField.apply(index)).dataType match {
    case ByteType =>
        typeCode match {
          case "SUM"=>
            if(isInit) {
              arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }else {
              arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index))) + originalArray.apply(index).asInstanceOf[Byte]
            }
          case "AVERAGE"=>
            if(isInit) {
              arrayData(index) = (row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
            }else {
              avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
              arrayData(index) = (row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))+avgdata._1.asInstanceOf[Byte],avgdata._2+1)
            }
          case "MEDIAN" | "PERCENTILE"=>
            if(isInit) {
              arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }else {
              percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
              percent += row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index))).toDouble
              arrayData(index) = percent
            }
          case "MIN" =>
            if(isInit) {
              arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }else {
              if(row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index))) < originalArray(index).asInstanceOf[Byte]){
                arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))
              }
            }
          case "MAX" =>
            if(isInit) {
              arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }else {
              if(row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index))) > originalArray(index).asInstanceOf[Byte]){
                arrayData(index) = row.getByte(row.fieldIndex(objectMeta.subjectField.apply(index)))
              }
            }

        }
    //: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
    case ShortType=>
      typeCode match {
        case "SUM"=>
          if(isInit) {
            arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index))) + originalArray.apply(index).asInstanceOf[Short]
          }
        case "AVERAGE"=>
          if(isInit) {
            arrayData(index) = (row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
          }else {
            avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
            arrayData(index) = (row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))+avgdata._1.asInstanceOf[Short],avgdata._2+1)
          }
        case "MEDIAN" | "PERCENTILE"=>
          if(isInit) {
            arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
            percent += row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index))).toDouble
            arrayData(index) = percent
          }
        case "MIN" =>
          if(isInit) {
            arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index))) < originalArray(index).asInstanceOf[Short]){
              arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
        case "MAX" =>
          if(isInit) {
            arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index))) > originalArray(index).asInstanceOf[Short]){
              arrayData(index) = row.getShort(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
      }
    //Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
    case IntegerType=>
      typeCode match {
        case "SUM"=>
          if(isInit) {
            arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index))) + originalArray.apply(index).asInstanceOf[Int]
          }
        case "AVERAGE"=>
          if(isInit) {
            arrayData(index) = (row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
          }else {
            avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
            arrayData(index) = (row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))+avgdata._1.asInstanceOf[Int],avgdata._2+1)
          }
        case "MEDIAN" | "PERCENTILE"=>
          if(isInit) {
            arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
            percent += row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index))).toDouble
            arrayData(index) = percent
          }
        case "MIN" =>
          if(isInit) {
            arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index))) < originalArray(index).asInstanceOf[Int]){
              arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
        case "MAX" =>
          if(isInit) {
            arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index))) > originalArray(index).asInstanceOf[Int]){
              arrayData(index) = row.getInt(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
      }
    //: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
    case LongType=>
      typeCode match {
        case "SUM"=>
          if(isInit) {
            arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index))) + originalArray.apply(index).asInstanceOf[Long]
          }
        case "AVERAGE"=>
          if(isInit) {
            arrayData(index) = (row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
          }else {
            avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
            arrayData(index) = (row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))+avgdata._1.asInstanceOf[Long],avgdata._2+1)
          }
        case "MEDIAN" | "PERCENTILE"=>
          if(isInit) {
            arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
            percent += row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index))).toDouble
            arrayData(index) = percent
          }

        case "MIN" =>
          if(isInit) {
            arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index))) < originalArray(index).asInstanceOf[Long]){
              arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
        case "MAX" =>
          if(isInit) {
            arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index))) > originalArray(index).asInstanceOf[Long]){
              arrayData(index) = row.getLong(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }

      }
    //: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
    case FloatType=>
      typeCode match {
        case "SUM"=>
          if(isInit) {
            arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index))) + originalArray.apply(index).asInstanceOf[Float]
          }
        case "AVERAGE"=>
          if(isInit) {
            arrayData(index) = (row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
          }else {
            avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
            arrayData(index) = (row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))+avgdata._1.asInstanceOf[Float],avgdata._2+1)
          }
        case "MEDIAN" | "PERCENTILE"=>
          if(isInit) {
            arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
            percent += row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index))).toDouble
            arrayData(index) = percent
          }
        case "MIN" =>
          if(isInit) {
            arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index))) < originalArray(index).asInstanceOf[Float]){
              arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
        case "MAX" =>
          if(isInit) {
            arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index))) > originalArray(index).asInstanceOf[Float]){
              arrayData(index) = row.getFloat(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
      }
    //: Represents 4-byte single-precision floating point numbers.
    case DoubleType=>
      typeCode match {
        case "SUM"=>
          if(isInit) {
            arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index))) + originalArray.apply(index).asInstanceOf[Double]
          }
        case "AVERAGE"=>
          if(isInit) {
            arrayData(index) = (row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
          }else {
            avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
            arrayData(index) = (row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))+avgdata._1.asInstanceOf[Double],avgdata._2+1)
          }
        case "MEDIAN" | "PERCENTILE"=>
          if(isInit) {
            arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
            percent += row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index))).toDouble
            arrayData(index) = percent
          }
        case "MIN" =>
          if(isInit) {
            arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index))) < originalArray(index).asInstanceOf[Double]){
              arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
        case "MAX" =>
          if(isInit) {
            arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index))) > originalArray(index).asInstanceOf[Double]){
              arrayData(index) = row.getDouble(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
      }
    //: Represents 8-byte double-precision floating point numbers.
    case DecimalType()=>
      typeCode match {
        case "SUM"=>
          if(isInit) {
            arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index))) add originalArray.apply(index).asInstanceOf[java.math.BigDecimal]
          }
        case "AVERAGE"=>
          if(isInit) {
            arrayData(index) = (row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index))),1L)
          }else {
            avgdata = originalArray.apply(index).asInstanceOf[Tuple2[Any,Long]]
            arrayData(index) = (row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index))) add avgdata._1.asInstanceOf[java.math.BigDecimal],avgdata._2+1)
          }
        case "MEDIAN" | "PERCENTILE"=>
          if(isInit) {
            arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            percent = originalArray.apply(index).asInstanceOf[ListBuffer[Double]]
            percent += row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index))).doubleValue()
            arrayData(index) = percent
          }
        case "MIN" =>
          if(isInit) {
            arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index))).compareTo(originalArray(index).asInstanceOf[java.math.BigDecimal]) == -1){
              arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
        case "MAX" =>
          if(isInit) {
            arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index)))
          }else {
            if(row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index))).compareTo(originalArray(index).asInstanceOf[java.math.BigDecimal]) == 1){
              arrayData(index) = row.getDecimal(row.fieldIndex(objectMeta.subjectField.apply(index)))
            }
          }
      }
    //: Represents arbitrary-precision signed decimal numbers. Backe
  }
  }

}
