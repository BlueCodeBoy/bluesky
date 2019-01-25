package com.bluesky.etl.steps.filterrows

import com.bluesky.etl.common.CommonSparkUtils
import org.apache.spark.sql.functions.negate
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.pentaho.di.core.{Condition, Const}
import org.pentaho.di.core.row.ValueMetaInterface
import org.pentaho.di.core.row.value.ValueMetaBase

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

import scala.collection.JavaConverters._
import scala.collection.mutable

class FilterRows {

    var condition: Column = null
    def calConditionWithDataFrame(dataFrame: DataFrame, conditions: Condition): Column = {
      calCondition(conditions,dataFrame)
      if(null == condition){
          null
      }else{
        if(conditions.isNegated){
          condition = negate(condition)
        }
        condition
      }
    }


    /**
      *
      * @param originalConditon
      * @param dataFrame
      */
    def calCondition(originalConditon: Condition,dataFrame: DataFrame):Unit={
        var tmpColumn:Column = null
        if(null == condition ){
            require(originalConditon.getOperator==0,"condition error")
            tmpColumn = concatColumn(dataFrame,originalConditon)
            if(null!=tmpColumn){
                condition = tmpColumn
            }
        }else{
            tmpColumn = concatColumn(dataFrame,originalConditon)
            if(null!=tmpColumn){
                Condition.operators.apply(originalConditon.getOperator) match {
                    case "-"=>
                    case "OR" =>
                        condition = condition or tmpColumn
                    case "AND"=>
                        condition = condition and tmpColumn
                    case "NOT"=>
                        condition =condition and negate( tmpColumn)
                    case "OR NOT"=>
                        condition = condition or negate(tmpColumn)
                    case "AND NOT"=>
                        condition = condition and negate(tmpColumn)
                    case "XOR" =>
                        condition = condition bitwiseXOR  tmpColumn
                }
            }
        }
        val buffer = originalConditon.getChildren.asScala
        if(!buffer.isEmpty){
            for(c<-buffer){
                calCondition(c,dataFrame)
            }
        }
    }
    /**
      *
      * @param dataFrame
      * @param condition
      * @return
      */
    def concatColumn(dataFrame: DataFrame,condition: Condition):Column= {
      Condition.functions.apply(condition.getFunction) match {
        case "=" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) === condition.getRightExact.getValueData
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) === dataFrame(condition.getRightValuename)
          }
        case "<>" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) notEqual condition.getRightExact.getValueData
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) notEqual dataFrame(condition.getRightValuename)
          }
        case "<" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) < condition.getRightExact.getValueData
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) < dataFrame(condition.getRightValuename)
          }
        case "<=" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) <= condition.getRightExact.getValueData
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) <= dataFrame(condition.getRightValuename)
          }
        case ">" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) > condition.getRightExact.getValueData
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) > dataFrame(condition.getRightValuename)
          }
        case ">=" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) >= condition.getRightExact.getValueData
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) >= dataFrame(condition.getRightValuename)
          }
        case "REGEXP" =>
          throw new RuntimeException("not supported(regexp)!!!!!!!!!!!!!!!!!")
        case "IS NULL" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) isNull
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) isNull
          }

        case "IS NOT NULL" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename) isNotNull
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename) isNotNull
          }

        case "IN LIST" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              val inList = Const.splitString(condition.getRightExact.getValueData.toString, ';', true).toBuffer
              var i: Int = 0
              while ( {
                i < inList.length
              }) {
                inList(i) = if (inList(i) == null) null
                else inList(i).replace("\\", "")

                {
                  i += 1;
                  i - 1
                }
              }
              dataFrame(condition.getLeftValuename).isin(inList: _*)
            } else {
              null
            }
          } else {
            throw new RuntimeException("not supported ( in list-fields)")
          }

        case "CONTAINS" =>
          throw new RuntimeException("not supported(CONTAINS)!!!!!!!!!!!!!!!!!")
        case "STARTS WITH" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename).startsWith(condition.getRightExact.getValueData.toString)
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename).startsWith(dataFrame(condition.getRightValuename))
          }

        case "ENDS WITH" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename).endsWith(condition.getRightExact.getValueData.toString)
            } else {
              null
            }
          } else {
            dataFrame(condition.getLeftValuename).endsWith(dataFrame(condition.getRightValuename))
          }
        case "LIKE" =>
          if (condition.getRightValuename == null) {
            if (condition.getRightExact != null) {
              dataFrame(condition.getLeftValuename).like(condition.getRightExact.getValueData.toString)
            } else {
              null
            }
          } else {
            throw new RuntimeException("not supported like column !!!!!!!!!!!!!!!!!")
            //dataFrame(condition.getLeftValuename).like(dataFrame(condition.getRightValuename))
          }
        case "TRUE" =>
          throw new RuntimeException("not supported (true)!!!!!!!!!!!!!!!!!")
      }

    }
}
