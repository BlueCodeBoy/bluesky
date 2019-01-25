package com.bluesky.etl.kettle;

import com.alibaba.fastjson.JSONObject;
import com.bluesky.etl.util.kettleenv.KettleEnv;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KettleUtil {


    /**
     * 方法名: getTransMetaById 方法功能描述: 通过资源库元数据及转换ID获取转换元数据
     *
     * @param
     * @return
     * @Author SF-2186
     * @Create Date: 2017/9/4 8:42
     */
    public static TransMeta getTransMetaByName(DatabaseMeta dbMeta, String tranName) throws Exception {
        List<Map<String, Object>> tranInfoList = new ArrayList<Map<String, Object>>();
        Repository repository = getRepository(dbMeta);
        if ((repository) == null) {
            throw new Exception("获取仓库失败!");
        }
        RepositoryDirectoryInterface directory = repository.findDirectory("/");
        TransMeta transMeta = repository.loadTransformation(tranName, directory, null, false, null);
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
    public static void saveTransMetaByName(DatabaseMeta dbMeta, TransMeta transMeta) throws Exception {
        Repository repository = getRepository(dbMeta);
        if ((repository) == null) {
            throw new Exception("获取仓库失败!");
        }
        RepositoryDirectoryInterface directory = repository.findDirectory("/");
        boolean versioningEnabled = true;
        boolean versionCommentsEnabled = true;
        String fullPath = transMeta.getRepositoryDirectory() + "/" + transMeta.getName() + transMeta.getRepositoryElementType().getExtension();
        RepositorySecurityProvider repositorySecurityProvider = repository.getSecurityProvider() != null?repository.getSecurityProvider():null;
        if(repositorySecurityProvider != null) {
            versioningEnabled = repositorySecurityProvider.isVersioningEnabled(fullPath);
            versionCommentsEnabled = repositorySecurityProvider.allowsVersionComments(fullPath);
        }

        String versionComment = null;
        if(versioningEnabled && versionCommentsEnabled) {
            versionComment = "no comment";
        } else {
            versionComment = "";
        }
        repository.save(transMeta,versionComment, (ProgressMonitorListener)null);

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
            KettleEnvironment.init();
            // 创建DB资源库
            repository = new KettleDatabaseRepository();
            // 选择资源库
            KettleDatabaseRepositoryMeta kettleDatabaseRepositoryMeta = new KettleDatabaseRepositoryMeta("kettle",
                    "kettle", "Transformation description", dbMeta);
            repository.init(kettleDatabaseRepositoryMeta);
            // 连接资源库
            repository.connect("admin", "admin");
        } catch (KettleException var2) {
            repository = null;
        } catch (Exception var4) {
            System.out.println("error not care :"+var4.getMessage());
            repository = null;
        }
        return repository;
    }
    public static DatabaseMeta getDatabaseMetaByaseString(JSONObject jsonObject){
        DatabaseMeta dbMeta = null;
        try {
            if ((jsonObject.get("tranceName")) != null) {
                String trancename = jsonObject.get("tranceName").toString();
                //设置仓库数据库连接信息
                KettleEnv.getIstance().setKettleRepositoryAccessInfo(jsonObject.get("databaseName") == null ? "" : jsonObject.get("databaseName").toString(),
                        jsonObject.get("hostname") == null ? "" : jsonObject.get("hostname").toString()
                        , jsonObject.get("port") == null ? "" : jsonObject.get("port").toString(),
                        jsonObject.get("user") == null ? "" : jsonObject.get("user").toString(),
                        jsonObject.get("password") == null ? "" : jsonObject.get("password").toString(),
                        jsonObject.get("type") == null ? "" : jsonObject.get("type").toString());
                //设置仓库登录信息
                KettleEnv.getIstance().setKettleRepositoryLoginInfo(jsonObject.get("etlRepositoryLoginUser") == null ? "" : jsonObject.get("etlRepositoryLoginUser").toString(), jsonObject.get("etlRepositoryLoginPass") == null ? "" : jsonObject.get("etlRepositoryLoginPass").toString());
                String fileName = Const.getKettleDirectory() + Const.FILE_SEPARATOR +Const.KETTLE_PROPERTIES;
                File file = new File(fileName);
                if(file.isFile()){
                    file.delete();
                }
                try{
                    //资源库自动登录初始化
                    KettleEnv.getIstance().kettleEnvInit();
                }catch (Exception e){
                    System.out.println("error not care :"+e.getMessage());
                }
                //                组装数据源数据
                dbMeta= new DatabaseMeta("etl",
                        jsonObject.get("type") == null ? "" : jsonObject.get("type").toString(), "0",
                        jsonObject.get("hostname") == null ? "" : jsonObject.get("hostname").toString(),
                        jsonObject.get("databaseName") == null ? "" : jsonObject.get("databaseName").toString(),
                        jsonObject.get("port") == null ? "" : jsonObject.get("port").toString(),
                        jsonObject.get("user") == null ? "" : jsonObject.get("user").toString(),
                        jsonObject.get("password") == null ? "" : jsonObject.get("password").toString()
                );

            }
        } catch (Exception e) {
            System.out.println("error not care :"+e.getMessage());
        }
        return dbMeta;
    }
    public static void saveTransMetaByaesString(JSONObject jsonObject,TransMeta transMeta) throws Exception {
        try {
            if ((jsonObject.get("tranceName")) != null) {
                String trancename = jsonObject.get("tranceName").toString();
                DatabaseMeta dbMeta = getDatabaseMetaByaseString(jsonObject);
                saveTransMetaByName(dbMeta,transMeta);

            }
        } catch (Exception e) {
            System.out.println("error not care :"+e.getMessage());
        }
    }
    public static TransMeta getTransMetaByaesString(JSONObject jsonObject) throws Exception {
        TransMeta transMeta = null;
        try {
            if ((jsonObject.get("tranceName")) != null) {
                String trancename = jsonObject.get("tranceName").toString();
                DatabaseMeta dbMeta = getDatabaseMetaByaseString(jsonObject);
                //从资源库获取转换元数据
                transMeta = KettleUtil.getTransMetaByName(dbMeta, trancename);

            }
        } catch (Exception e) {
            System.out.println("error not care :"+e.getMessage());
        }
        return transMeta;
    }


}
