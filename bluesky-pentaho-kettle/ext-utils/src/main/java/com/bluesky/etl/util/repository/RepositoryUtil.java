package com.bluesky.etl.util.repository;


import com.bluesky.etl.util.EtlApp;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.*;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by SF-2186 on 2017/4/12.
 */
public class RepositoryUtil {
    //列出仓库所有数据源列表
    public static List<RepositoryMeta> listAllRepository() throws Exception{
        RepositoriesMeta input = new RepositoriesMeta();
        List<RepositoryMeta> repositoryMetaArrayList=new ArrayList<>();
        if (input.readData()) {
            for (int i = 0; i < input.nrRepositories(); i++) {
                repositoryMetaArrayList.add(input.getRepository(i));
            }
        }
        return repositoryMetaArrayList;
    }

    //创建目录
    public static String createDir(Repository repository, String dir, String name) throws KettleException, IOException {
        RepositoryDirectoryInterface path = repository.findDirectory(new StringObjectId(dir));
        RepositoryDirectoryInterface newdir = repository.createRepositoryDirectory(path, name);

        return newdir.getObjectId().getId();
    }

    //创建空转换转换
    public static String createEmptyTrans(Repository repository,String dir, String transName) throws KettleException, IOException {
        RepositoryDirectoryInterface path = repository.findDirectory(new StringObjectId(dir));
        if (path == null) {
            path = repository.getUserHomeDirectory();
        }
        if (repository.exists(transName, path, RepositoryObjectType.TRANSFORMATION)) {
            return "";
        }

        TransMeta transMeta = new TransMeta();
        transMeta.setRepository(repository);
        transMeta.setMetaStore(repository.getMetaStore());
        transMeta.setName(transName);
        transMeta.setRepositoryDirectory(path);
        repository.save(transMeta, "add: " + new Date(), null);
        ObjectId id = repository.getTransformationID(transName, path);
       return id.toString();
    }

    //创建空任务
    public static String createEmptyJob(Repository repository,String dir,String jobName) throws KettleException, IOException {
        RepositoryDirectoryInterface path = repository.findDirectory(new StringObjectId(dir));

        if (repository.exists(jobName, path, RepositoryObjectType.JOB)) {
            return "";
        }

        JobMeta jobMeta = new JobMeta();
        jobMeta.setRepository(repository);
        jobMeta.setMetaStore(repository.getMetaStore());
        jobMeta.setName(jobName);
        jobMeta.setRepositoryDirectory(path);

        repository.save(jobMeta, "add: " + new Date(), null);

        ObjectId id = repository.getJobId(jobName, path);
        return  id.getId().toString();
    }

    //增加资源库
    public static boolean addRepository(RepositoryMeta repositoryMeta, boolean add) throws IOException, KettleException {
        Repository reposity = PluginRegistry.getInstance().loadClass( RepositoryPluginType.class,  repositoryMeta, Repository.class );
        reposity.init( repositoryMeta );

            RepositoriesMeta input = new RepositoriesMeta();
            input.readData();

            if(add) {
                if(input.searchRepository(repositoryMeta.getName()) != null) {
					return true;
                } else {
                    input.addRepository(repositoryMeta);
                    input.writeData();
                }
            } else {
                RepositoryMeta previous = input.searchRepository(repositoryMeta.getName());
                input.removeRepository(input.indexOfRepository(previous));
                input.addRepository(repositoryMeta);
                input.writeData();
            }
        return true;
    }

    //删除资源库
    public static boolean removeRepository(String repositoryName) throws KettleException, IOException {
        RepositoriesMeta input = new RepositoriesMeta();
        input.readData();

        RepositoryMeta previous = input.searchRepository(repositoryName);
        input.removeRepository(input.indexOfRepository(previous));
        input.writeData();
        return true;
    }

    //登录仓库
    public static boolean login(RepositoryMeta repMeta,String userName,String passWord,boolean atStartupShown) throws IOException, KettlePluginException, KettleSecurityException, KettleException {
        RepositoriesMeta input = new RepositoriesMeta();
        if (input.readData()) {
            RepositoryMeta repositoryMeta = input.searchRepository(repMeta.getName());
            if (repositoryMeta != null) {
                Repository repository = PluginRegistry.getInstance().loadClass(RepositoryPluginType.class, repositoryMeta.getId(), Repository.class);
                repository.init(repositoryMeta);
                repository.connect(userName, passWord);

                Props.getInstance().setLastRepository(repositoryMeta.getName());
                Props.getInstance().setLastRepositoryLogin(userName);
                Props.getInstance().setProperty(PropsUI.STRING_START_SHOW_REPOSITORIES, atStartupShown ? "Y" : "N");
                Props.getInstance().saveProps();

                EtlApp.getInstance().selectRepository(repository);
                EtlApp.getInstance().setLogined(true);
            }
        }
        return true;
    }


    //退出登录
    public static boolean logout() throws IOException {
        Repository repo=EtlApp.getInstance().getRepository();
        if(repo != null) {
            repo.disconnect();
            EtlApp.getInstance().setLogined(false);
        }
        return true;
    }
}
