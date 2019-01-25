package com.bluesky.etl.util;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.*;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by SF-2186 on 2018/5/21.
 */
public class KettleUtil {
    /**
     * 方法名: getTransMetaById 方法功能描述: 通过资源库元数据及转换ID获取转换元数据
     *
     * @param
     * @return
     * @Author SF-2186
     * @Create Date: 2017/9/4 8:42
     */
    public static TransMeta getTransMetaById(DatabaseMeta dbMeta, String transId) throws Exception {
        List<Map<String, Object>> tranInfoList = new ArrayList<>();
        Repository repository = getRepository(dbMeta);
        if (null==repository) {
            throw new Exception("获取仓库失败!");
        }
        ObjectId id = new StringObjectId(transId);

        RepositoryObject repositoryObject = repository.getObjectInformation(id, RepositoryObjectType.TRANSFORMATION);

        TransMeta transMeta = repository.loadTransformation(id, null);
        transMeta.setRepositoryDirectory(repositoryObject.getRepositoryDirectory());
        repository.disconnect();// 断开资源库连接

        return transMeta;
    }

    /**
     * 方法名: getTransMetaById 方法功能描述: 通过资源库元数据及转换ID获取转换元数据
     *
     * @param
     * @return
     * @Author SF-2186
     * @Create Date: 2017/9/4 8:42
     */
    public static TransMeta getTransMetaByName(DatabaseMeta dbMeta, String tranName) throws Exception {
        List<Map<String, Object>> tranInfoList = new ArrayList<>();
        Repository repository = getRepository(dbMeta);
        if (null==repository) {
            throw new Exception("获取仓库失败!");
        }
        RepositoryDirectoryInterface directory = repository.findDirectory("/");
        TransMeta transMeta = repository.loadTransformation(tranName, directory, null, false, null);
        repository.disconnect();// 断开资源库连接

        return transMeta;
    }

    public static JobMeta getJobMetaById(DatabaseMeta dbMeta, String jobName) throws Exception {
        Repository repository = getRepository(dbMeta);
        if (null==repository) {
            throw new Exception("获取仓库失败!");
        }
        RepositoryDirectoryInterface directory = repository.findDirectory("/");
        JobMeta jobMeta = repository.loadJob(String.valueOf(jobName), directory, null, null);
        repository.disconnect();// 断开资源库连接
        return jobMeta;
    }

    /**
     * 方法名: getTranInfoByDbMeta 方法功能描述: 通过资源库元数据及目录获取包含的转换信息列表
     *
     * @param
     * @return
     * @Author SF-2186
     * @Create Date: 2017/9/4 8:42
     */
    public static List<Map<String, Object>> getTranInfoByDbMeta(DatabaseMeta dbMeta, String dir) throws Exception {
        List<Map<String, Object>> tranInfoList = new ArrayList<>();
        Repository repository = getRepository(dbMeta);
        if (null==repository) {
            throw new Exception("获取仓库失败!");
        }
        RepositoryDirectoryInterface path = repository.findDirectory(new StringObjectId(dir));
        if (path == null) {
            path = repository.getUserHomeDirectory();
        }
        List<RepositoryElementMetaInterface> elements = repository.getTransformationObjects(path.getObjectId(), false);
        for (RepositoryElementMetaInterface repositoryElementMetaInterface : elements) {
            Map<String, Object> map = new HashMap<>();
            map.put("tranName", repositoryElementMetaInterface.getName());
            map.put("tranId", repositoryElementMetaInterface.getObjectId().getId());
            tranInfoList.add(map);
        }
        repository.disconnect();// 断开资源库连接

        return tranInfoList;
    }

    /**
     * 方法名: getRepository 方法功能描述: 根据数据库连接元数据获取资源库
     *
     * @param
     * @return
     * @Author SF-2186
     * @Create Date: 2017/8/29 17:53
     */
    public static Repository getRepository(DatabaseMeta dbMeta) {
        KettleDatabaseRepository repository = null;
        try {
            kettleEnviInit();
            // 创建DB资源库
            repository = new KettleDatabaseRepository();
            // 选择资源库
            KettleDatabaseRepositoryMeta kettleDatabaseRepositoryMeta = new KettleDatabaseRepositoryMeta("kettle",
                    "kettle", "Transformation description", dbMeta);
            repository.init(kettleDatabaseRepositoryMeta);
            // 连接资源库
            repository.connect("admin", "admin");
        } catch (KettleException var2) {
            var2.printStackTrace();
            repository = null;
        } catch (Exception var4) {
            var4.printStackTrace();
            repository = null;
        }
        return repository;
    }

    public static void  kettleEnviInit() throws Exception{
        try {
            KettleLogStore.init(5000, 720);
            PropsWebUI.init("KettleWebConsole", 4);
            KettleEnvironment.init();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("kettle环境初始化错误");
            //throw e;
        }
    }

}
