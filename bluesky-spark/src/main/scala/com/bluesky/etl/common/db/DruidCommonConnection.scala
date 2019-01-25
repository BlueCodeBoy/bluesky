package com.bluesky.etl.common.db

import java.sql.Connection
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import com.alibaba.druid.pool.DruidDataSource
import com.bluesky.etl.utils.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.convert.decorateAsScala._
import scala.collection._
/**
  * Created by root on 18-11-23.
  */
object DruidCommonConnection extends Logging{
  val data: concurrent.Map[String, DruidDataSource] = new ConcurrentHashMap[String,DruidDataSource]().asScala
//  new ArrayBuffer[DruidDataSource](20)
  /**
    * 最大连接数
    */
  val MAX_POOL_SIZE = 300

  /**
    * 最小连接数
    */
  val MIN_POOL_SIZE = 10

  /**
    * 初始化连接数
    */
  val INITIAL_POOL_SIZE = 5

  /**
    *
    * @param uuid
    * @return
    */
  def getDataSourceByuuid(uuid:String):DruidDataSource={
    data.getOrElse(uuid,null)
  }

  /**
    *
    * @param druidDataSource
    * @return
    */
  def getConnect(druidDataSource: DruidDataSource):Connection={
    var conn:Connection = null
    try{
      conn = druidDataSource.getConnection
    }catch {
      case e:Exception=>{
        logError(s"error while get Connection:${e.getMessage}")
      }
    }
    conn
  }

  @throws[Exception]
  def createConnectionPool(driver: String, url: String, userName: String, pwd: String, dbtype:String ,uuid:String): DruidDataSource = {
    if(null == data.getOrElse(uuid,null)) {
      val cpds = new DruidDataSource(true)
      cpds.setUrl(url)
      try
        cpds.setDriverClassName(driver)
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
      cpds.setUsername(userName)
      cpds.setPassword(pwd)
      cpds.setInitialSize(INITIAL_POOL_SIZE) //初始化连接大小

      cpds.setMinIdle(MIN_POOL_SIZE) //连接池最小空闲

      cpds.setMaxActive(MAX_POOL_SIZE) //连接池最大使用链接数量

      cpds.setMaxWait(90000) //配置获取连接等待超时的时间

      cpds.setTimeBetweenEvictionRunsMillis(5000) //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒

      <!-- 配置一个连接在池中最小生存的时间，单位是毫秒 -->
      cpds.setMinEvictableIdleTimeMillis(30000) //配置一个连接在池中最小生存的时间，单位是毫秒
      cpds.setMaxEvictableIdleTimeMillis(900000)

      cpds.setValidationQuery(createValidationQuery(dbtype))
      cpds.setTestWhileIdle(true)
      cpds.setTestOnBorrow(false)
      cpds.setTestOnReturn(false)
      //        if (dbtype == ResourceTypeEnum.DM || dbtype == ResourceTypeEnum.HIVE) {
      //            cpds.setFilters("stat");
      //        } else {
      cpds.setFilters("wall")
      cpds.setConnectProperties(getProperties(dbtype))
      //        }
      //申请的连接忘记关闭，这时候，就存在连接泄漏了。Druid提供了RemoveAbandanded相关配置，用来关闭长时间不使用的连接
      //cpds.setRemoveAbandoned(true);//打开removeAbandoned功能
      //cpds.setRemoveAbandonedTimeout(120);//2分钟  SF2548 改动，影响共享同步数据
      cpds.setLogAbandoned(true) //关闭abanded连接时输出错误日志
      data.putIfAbsent(uuid, cpds)
      cpds
    }else{
      data.get(uuid).get
    }
  }
  private def getProperties(dbType:String)={
    val properties:Properties = new Properties()
    dbType match {
      case "MYSQL" =>
      case "ORACLE" =>
        properties.put("oracle.net.CONNECT_TIMEOUT", "10000000")
        properties.put("oracle.jdbc.ReadTimeout", "2000")
      //            case PostgreSQL:
      //                querySql = "SELECT 1";
      //                break;
      //            case DB2:
      //                querySql = "SELECT 1 FROM SYSIBM.SYSDUMMY1";
      //                break;
      case "MSSQL" =>
      //            case HIVE:
      case _ =>
    }
    properties
  }
  /**
    *
    * @param dbType
    * @return
    */
  private def createValidationQuery(dbType: String) = {
    var querySql = " "
    dbType match {
      case "MYSQL" =>
        querySql = "SELECT 1"
      case "ORACLE" =>
        querySql = "SELECT 1 FROM DUAL"
      //            case PostgreSQL:
      //                querySql = "SELECT 1";
      //                break;
      //            case DB2:
      //                querySql = "SELECT 1 FROM SYSIBM.SYSDUMMY1";
      //                break;
      case "MSSQL" =>
        querySql = "SELECT 1"
      //            case HIVE:
      case _ =>
    }
    querySql
  }
}
