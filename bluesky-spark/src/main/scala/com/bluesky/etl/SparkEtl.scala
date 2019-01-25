package com.bluesky.etl

import java.io._
import java.util.jar.JarOutputStream
import java.util.zip.{ZipEntry, ZipFile, ZipInputStream, ZipOutputStream}

import com.alibaba.fastjson.JSONObject
import com.bluesky.etl.kettle.KettleUtil
import com.bluesky.etl.core.trans.Trans
import com.bluesky.etl.utils.{AESEncrypt, Logging}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.pentaho.di.trans.TransMeta
import scopt.OptionParser

import scala.util.control.Breaks.{break, breakable}
object SparkEtl extends Logging {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]): Unit = {
    // --sparkAppname "11" --transName "test1(10.0.8.183@21066)_emp_test_20180903112434" --dataBaseName "etl_web_quality" --hostName "10.0.8.202" --port "3306" --user "root" --password "XvdLS/stugtARhFCIZyTjg==" --Dbtype "MYSQL" --loginUser "admin" --loginPassword "ENOSChKk0iGrZBpMKiN9rQ==" --qualityBaseUrl "http://10.0.8.61:8082/wydataquality/"
    val parser = parseArgs("sparkEtl-lighter")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>
        // --sparkAppname         "11"   --transName         "emp(10.0.8.145@10000)_emp_20180817082213"   --dataBaseName         "etl_web_quality"   --hostName         "10.0.8.202"   --port         "3306"   --user         "root"   --password         "XvdLS/stugtARhFCIZyTjg=="   --Dbtype         "MYSQL"   --loginUser         "admin"   --loginPassword   "ENOSChKk0iGrZBpMKiN9rQ=="
//        System.setProperty("java.security.auth.login.config", "/etc/sparkjaas.conf")
//        System.setProperty("sun.security.jgss.debug", "true")
//        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
//        System.setProperty("hadoop.security.authentication", "kerberos")
//        System.setProperty("hadoop.security.authorization", "true")
        logInfo("starting extract file ")
        extractFile()
        logInfo("end extract file ")
        logInfo("---------sparkAppname-----------" + params.sparkAppname)
        logInfo("---------transName-----------" + params.transName)
        logInfo("---------dataBaseName-----------" + params.dataBaseName)
        logInfo("---------hostName-----------" + params.hostName)
        logInfo("---------port-----------" + params.port)
        logInfo("---------user-----------" + params.user)
        logInfo("---------password-----------" + params.password)
        logInfo("---------Dbtype-----------" + params.Dbtype)
        logInfo("---------loginUser-----------" + params.loginUser)
        logInfo("---------loginPassword-----------" + params.loginPassword)
//        logInfo("---------qualityBaseUrl-----------" + params.qualityBaseUrl)
        val jsonObject = new JSONObject
        val aesEncrypt = new AESEncrypt
        var dbpwd = params.password
        var loginpwd = params.loginPassword
        try
          dbpwd = aesEncrypt.decrypt(params.password)
        catch {
          case t: Exception =>
        }
        try
          loginpwd = aesEncrypt.decrypt(params.loginPassword)
        catch {
          case t: Exception =>
        }
        jsonObject.put("databaseName", params.dataBaseName)
        jsonObject.put("hostname", params.hostName)
        jsonObject.put("port", params.port)
        jsonObject.put("user", params.user)
        jsonObject.put("password", dbpwd)
        jsonObject.put("type", params.Dbtype)
        jsonObject.put("etlRepositoryLoginUser", params.loginUser)
        jsonObject.put("etlRepositoryLoginPass", loginpwd)
        jsonObject.put("tranceName",params.transName)
        val transMeta = KettleUtil.getTransMetaByaesString(jsonObject)
        val sparkConf = new SparkConf().setAppName(params.sparkAppname)
        sparkConf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        setStreamingParams(sparkConf,transMeta)
        if(null!=transMeta.getParameterDefault("streaming.run.local")&&transMeta.getParameterDefault("streaming.run.local").toString.equals("true")) {
          var thread = 2
          if(null!=transMeta.getParameterDefault("streaming.run.local.thread")) {
            try{
              thread = transMeta.getParameterDefault("streaming.run.local.thread").toInt
            }catch {
              case e:Exception=>
            }
          }
          sparkConf.setMaster(s"local[${thread}]")
        }
        val sparkSession = SparkSession.builder().enableHiveSupport().appName(params.sparkAppname).config(sparkConf).getOrCreate()

        var trans = new Trans()
        //init params
//        logInfo("Start preparing parameters")
        trans.initParams(transMeta, null,params.jobID)
        logInfo("End preparation parameters")
        //pripare exe
        logInfo("starting thread!")
        trans.prepareExecution(transMeta,sparkSession)
        //start thread
        trans.fireAllThread(transMeta, sparkSession)
        logInfo("end  thread!")
        //move data and statstic
        logInfo("starting MoveDataAndStatistics")
        trans.startMoveDataAndStatistics(sparkSession)
        logInfo("end MoveDataAndStatistics")
        //save transmeta
        if (trans.getisIncrement()) {
          transMeta.setAttributesMap(trans.getAttributesMap)
          KettleUtil.saveTransMetaByaesString(jsonObject, transMeta)
        }
        logInfo("trance execute successed")
        sparkSession.stop()
    }
  }

  /**
    *
    * @param sparkConf
    * @param tm
    */
  def setStreamingParams(sparkConf: SparkConf, tm:TransMeta):Unit={
    //spark.ui.retainedJobs->50
    val retainedJos = tm.getParameterDefault("spark.ui.retainedJobs")
    if(null!=retainedJos){
      sparkConf.set("spark.ui.retainedJobs",retainedJos.toString)
    }else{
      sparkConf.set("spark.ui.retainedJobs","50")
    }

    //spark.ui.retainedStages->100
    val retainedStages = tm.getParameterDefault("spark.ui.retainedStages")
    if(null!=retainedStages){
      sparkConf.set("spark.ui.retainedStages",retainedStages.toString)
    }else{
      sparkConf.set("spark.ui.retainedStages","100")
    }
    //spark.ui.retainedTasks->100
    val reatainTasks = tm.getParameterDefault("spark.ui.retainedTasks")
    if(null!=reatainTasks){
      sparkConf.set("spark.ui.retainedTasks",reatainTasks.toString)
    }else{
      sparkConf.set("spark.ui.retainedTasks","100")
    }
    //spark.worker.ui.retainedExecutors->50
    val reatainExecutors = tm.getParameterDefault("spark.worker.ui.retainedExecutors")
    if(null!=reatainExecutors){
      sparkConf.set("spark.worker.ui.retainedExecutors",reatainExecutors.toString)
    }else{
      sparkConf.set("spark.worker.ui.retainedExecutors","50")
    }
    //spark.worker.ui.retainedDrivers->50
    val retainDrivers = tm.getParameterDefault("spark.worker.ui.retainedDrivers")
    if(null!=retainDrivers){
      sparkConf.set("spark.worker.ui.retainedDrivers",retainDrivers.toString)
    }else{
      sparkConf.set("spark.worker.ui.retainedDrivers","50")
    }
    //spark.sql.ui.retainedExecutions->50
    val reatainExecutions = tm.getParameterDefault("spark.sql.ui.retainedExecutions")
    if(null!=reatainExecutions){
      sparkConf.set("spark.sql.ui.retainedExecutions",reatainExecutions.toString)
    }else{
      sparkConf.set("spark.sql.ui.retainedExecutions","50")
    }
    //spark.streaming.ui.retainedBatches->50
    val reatainBatches = tm.getParameterDefault("spark.streaming.ui.retainedBatches")
    if(null!=reatainBatches){
      sparkConf.set("spark.streaming.ui.retainedBatches",reatainBatches.toString)
    }else{
      sparkConf.set("spark.streaming.ui.retainedBatches","50")
    }
  }
  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"
      opt[String]("sparkAppname") required() action { (data, conf) =>
        conf.copy(sparkAppname = data)
      } text "sparkappname eg: test spark"
      opt[String]("transName") required() action { (data, conf) =>
        conf.copy(transName = data)
      } text "transName eg: test spark"
      opt[String]("dataBaseName") required() action { (data, conf) =>
        conf.copy(dataBaseName = data)
      }
      opt[String]("hostName") required() action { (data, conf) =>
        conf.copy(hostName = data)
      }
      opt[String]("port") required() action { (data, conf) =>
        conf.copy(port = data)
      }
      opt[String]("user") required() action { (data, conf) =>
        conf.copy(user = data)
      }
      opt[String]("password") required() action { (data, conf) =>
        conf.copy(password = data)
      }
      opt[String]("Dbtype") required() action { (data, conf) =>
        conf.copy(Dbtype = data)
      }
      opt[String]("loginUser") required() action { (data, conf) =>
        conf.copy(loginUser = data)
      }
      opt[String]("loginPassword") required() action { (data, conf) =>
        conf.copy(loginPassword = data)
      }
      opt[String]("jobID") required() action { (data, conf) =>
        conf.copy(jobID = data)
      }
//      opt[String]("qualityBaseUrl") required() action { (data, conf) =>
//        conf.copy(qualityBaseUrl = data)
//      }
    }
  }

  def extractFile():Unit={
    val rfile = new File(SparkEtl.getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
    var jarWholePath = SparkEtl.this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    try{
      jarWholePath = java.net.URLDecoder.decode(jarWholePath, "UTF-8")
    }
    catch {
      case e: UnsupportedEncodingException =>
        System.out.println(e.toString)
    }
    if(jarWholePath.endsWith(".jar")){
      logInfo(s"the jar path is ${jarWholePath}")
        val jarPath = new File(jarWholePath).getParentFile.getAbsolutePath
        val fis = new ZipFile(jarWholePath)
        val fin = new FileInputStream(jarWholePath)
        val bin = new BufferedInputStream(fin)
        val filePath = new  File(jarPath+ File.separator+"plugins/")
        logInfo(s"isdir:${filePath.isDirectory}  dir is ${filePath.getAbsolutePath}")
        if(!filePath.isDirectory){
          val b = filePath.mkdir()
          logInfo(s"mkdir result is ${b}")
        }
        val zin = new ZipInputStream(bin)
        var ze:ZipEntry = null
        var out:OutputStream = null
      var file:File = null
      try{
        breakable {
          while ( {
            (ze = zin.getNextEntry) != null
          })  {
            if(ze==null){
              break()
            }
            if(ze.getName.startsWith("plugins/")&&ze.getName.endsWith(".jar")){
              logInfo(s" starting extract file name :${ze.getName}")
              file = new File(jarPath+File.separator+ze.getName)
              if(file.exists()){
                file.delete()
              }
              out = new FileOutputStream(file)
              val buffer = new Array[Byte](9000)
              var len = 0
              breakable{
                while ( {
                  (len = zin.read(buffer)) != -1
                }){
                  if(len!= -1){
                    out.write(buffer, 0, len)
                  }else{
                    break()
                  }
                }
              }
              logInfo(s"the extract file in ${jarPath+File.separator+ze.getName}")
            }
          }
        }
      }finally {
        zin.close()
        bin.close()
        fin.close()
        fis.close()
      }
      logInfo("end extract in method")
    }
  }
  case class CLIParams(sparkAppname: String = "sparkAppname",
                       transName: String = "transName",
                       dataBaseName: String = "dataBaseName",
                       hostName: String = "hostName",
                       port: String = "port",
                       user: String = "user",
                       password: String = "password",
                       Dbtype: String = "Dbtype",
                       loginUser: String = "loginUser",
                       loginPassword: String = "dataBaseName",
                       jobID: String = "jobID"
//                       qualityBaseUrl: String = "dataBaseName"
                      )


}
