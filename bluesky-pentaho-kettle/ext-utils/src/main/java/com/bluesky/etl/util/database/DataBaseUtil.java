package com.bluesky.etl.util.database;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.repository.RepositoriesMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by SF-2186 on 2017/4/12.
 */
public class DataBaseUtil {

    //列出仓库所有数据源列表
    public static List<DatabaseMeta> listAllDataBase(RepositoriesMeta repositoriesMeta) throws Exception{

        List<DatabaseMeta> databaseMetaList=new ArrayList<>();
        if (repositoriesMeta.readData()) {
            for (int i = 0; i < repositoriesMeta.nrDatabases(); i++) {
                databaseMetaList.add(repositoriesMeta.getDatabase(i));
            }
        }
        return databaseMetaList;
    }

    //添加数据连接信息
    public static boolean addDataBase(DatabaseMeta databaseMeta) throws Exception{
        RepositoriesMeta repositories = new RepositoriesMeta();
        if(repositories.readData()) {
            DatabaseMeta previousMeta = repositories.searchDatabase(databaseMeta.getName());
            if(previousMeta != null) {
                repositories.removeDatabase(repositories.indexOfDatabase(previousMeta));
            }
            repositories.addDatabase( databaseMeta );
            repositories.writeData();
            return true;
        }
        return false;
    }

    //删除数据库连接
    public static boolean deleteDataBase(DatabaseMeta databaseMeta) throws Exception{
        RepositoriesMeta repositories = new RepositoriesMeta();
        if(repositories.readData()) {
                DatabaseMeta previousMeta = repositories.searchDatabase(databaseMeta.getName());
                if (previousMeta != null) {
                    repositories.removeDatabase(repositories.indexOfDatabase(previousMeta));
                }
            repositories.writeData();
            return true;
        }
        return false;
    }


}
