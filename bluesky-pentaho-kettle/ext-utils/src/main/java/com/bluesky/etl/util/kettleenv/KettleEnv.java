package com.bluesky.etl.util.kettleenv;

import com.bluesky.etl.util.PropsWebUI;
import com.bluesky.etl.util.database.DataBaseUtil;
import com.bluesky.etl.util.repository.RepositoryUtil;
import com.bluesky.etl.util.EtlApp;
import com.bluesky.etl.util.common.AESEncrypt;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogStatus;
import org.pentaho.di.core.logging.StepLogTable;
import org.pentaho.di.core.logging.TransLogTable;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;

import java.io.IOException;

/**
 * Created by SF-2186 on 2017/4/12.
 */
public class KettleEnv {
    private String                    databaseName;
    private String                    hostname;
    private String                    port;
    private String                    user;
    private String                    password;
    private String                    type;

    private String                    etlRepositoryLoginUser;
    private String                    etlRepositoryLoginPass;

    //仓库元数据
    private RepositoryMeta repositoryMeta;

    // 定义一个私有构造方法
    public KettleEnv() {
        System.out.println("KettleEnv构造方法");
    }
    //定义一个静态私有变量(不初始化，不使用final关键字，使用volatile保证了多线程访问时instance变量的可见性，避免了instance初始化时其他变量属性还没赋值完时，被另外线程调用)
    private static volatile KettleEnv instance;

    //定义一个共有的静态方法，返回该类型实例
    public static KettleEnv getIstance() {
        // 对象实例化时与否判断（不使用同步代码块，instance不等于null时，直接返回对象，提高运行效率）
        if (instance == null) {
            //同步代码块（对象未初始化时，使用同步代码块，保证多线程访问时对象在第一次创建后，不再重复被创建）
            synchronized (KettleEnv.class) {
                //未初始化，则初始instance变量
                if (instance == null) {
                    instance = new KettleEnv();
                }
            }
        }
        return instance;
    }

    /**
     * 方法名:         kettleEnvInit
     * 方法功能描述:    kettle环境初始化
     * @param
     * @return
     * @Author        SF-2186
     * @Create Date:  2017/5/24  9:40
     */
    public void kettleEnvInit(){
        try {
            // 日志缓冲不超过5000行，缓冲时间不超过720秒
            KettleLogStore.init( 5000, 720 );
            KettleEnvironment.init();
//			Props.init( Props.TYPE_PROPERTIES_KITCHEN );
            PropsWebUI.init( "KettleWebConsole", Props.TYPE_PROPERTIES_KITCHEN );

            //1、创建数据库连接配置
            DataBaseUtil.addDataBase(getDefaultDatabaseInfo());

            boolean add = findRepository(KettleEnvPara.ETLWEB_REPOSITORY_NAME);

            repositoryMeta=getDefaultReposityInfo();
            //2、创建资源库配置
            RepositoryUtil.addRepository(repositoryMeta,false);

            //3、登录到资源库
            RepositoryUtil.login(repositoryMeta,etlRepositoryLoginUser,etlRepositoryLoginPass,true);

            //4、自动创建转换日志表
            autoCreateTranLogTable();

        } catch (KettleException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception ex){
            ex.printStackTrace();
        }

    }

    //查找资源库是否已经存在
    private boolean findRepository(String repositoryName) throws KettleException,IOException{
        RepositoriesMeta input = new RepositoriesMeta();
        boolean retValue=false;
        if (input.readData()) {
            for (int i = 0; i < input.nrRepositories(); i++) {
                RepositoryMeta repositoryMeta = input.getRepository(i);
                if(KettleEnvPara.ETLWEB_REPOSITORY_NAME.equals(repositoryMeta.getName())){
                    retValue=true;
                    break;
                }
            }
        }
        return retValue;
    }

    //获取默认的数据库连接信息
    public DatabaseMeta getDefaultDatabaseInfo() {

        String password1=password;
        try {
            password1=new AESEncrypt().decrypt(password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        DatabaseMeta databaseMeta = new DatabaseMeta();

        databaseMeta.setName(KettleEnvPara.ETLWEB_CONNECT_NAME);
        databaseMeta.setDisplayName(databaseMeta.getName());
        databaseMeta.setDatabaseType(type);
        databaseMeta.setAccessType(0);

            databaseMeta.setHostname(hostname);
            databaseMeta.setDBName(databaseName);
            databaseMeta.setUsername(user);
            databaseMeta.setPassword(password1);
        databaseMeta.setDBPort(port);
        databaseMeta.setSupportsBooleanDataType(false);
        databaseMeta.setSupportsTimestampDataType(true);
        databaseMeta.setPreserveReservedCase(true);
        databaseMeta.setForcingIdentifiersToLowerCase(false);//小写检查
        databaseMeta.setForcingIdentifiersToUpperCase(true);
        //扩展选项
        databaseMeta.addExtraOption(databaseMeta.getPluginId(), "defaultFetchSize", "500");
        databaseMeta.addExtraOption(databaseMeta.getPluginId(), "autoReconnect", "true");
        //databaseMeta.addExtraOption(databaseMeta.getPluginId(), "connectTimeout", "300");
        //databaseMeta.addExtraOption(databaseMeta.getPluginId(), "socketTimeout", "300");

        return databaseMeta;
    }

    //获取默认的仓库信息
    public RepositoryMeta getDefaultReposityInfo() throws Exception{
        RepositoryMeta repositoryMeta = PluginRegistry.getInstance().loadClass( RepositoryPluginType.class, "KettleDatabaseRepository", RepositoryMeta.class );
        repositoryMeta.setName(KettleEnvPara.ETLWEB_REPOSITORY_NAME);
        repositoryMeta.setDescription(KettleEnvPara.ETLWEB_REPOSITORY_NAME);
        if(repositoryMeta instanceof KettleDatabaseRepositoryMeta) {
            KettleDatabaseRepositoryMeta kettleDatabaseRepositoryMeta = (KettleDatabaseRepositoryMeta) repositoryMeta;

            RepositoriesMeta input = new RepositoriesMeta();
            if (input.readData()) {
                DatabaseMeta connection = input.searchDatabase(KettleEnvPara.ETLWEB_CONNECT_NAME);
                kettleDatabaseRepositoryMeta.setConnection(connection);
            }
        }
        return repositoryMeta;
    }

    //系统自动创建转换日志表
    private void autoCreateTranLogTable(){
        try {
            Repository repository = EtlApp.getInstance().getRepository();
            //List<DatabaseMeta> lstDbMeta = repository.readDatabases();
            DatabaseMeta dbMeta=((KettleDatabaseRepository) repository).connectionDelegate.getDatabaseMeta();
            Database db = new Database(null, dbMeta);
            try {


                if (dbMeta!=null) {
                    db.connect();
                    if (!db.checkTableExists(KettleEnvPara.ETLWEB_TRANSLOG_TABLE)) {
                        //创建转换日志表

                        String ddlSql = getTranCreateTableSQL(db);
                        if (!ddlSql.isEmpty()) {
                            db.execStatement(ddlSql);
                        }
                    }
                    if (!db.checkTableExists(KettleEnvPara.ETLWEB_STEPLOG_TABLE)) {
                        //创建步骤日志表

                        String ddlSql = getStepCreateTableSQL(db);
                        if (!ddlSql.isEmpty()) {
                            db.execStatement(ddlSql);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("转换或步骤日志数据表初始化失败:" + e.getMessage());
                //throw e;
            } finally {
                if (db != null) {
                    db.disconnect();
                }
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    //生成转换日志表sql语句
    private String getTranCreateTableSQL(Database db) throws  Exception{
        TransMeta transMeta = new TransMeta();
        TransLogTable logTable = (TransLogTable) transMeta.getTransLogTable().clone();
        StringBuilder ddl = new StringBuilder();

        RowMetaInterface fields = logTable.getLogRecord( LogStatus.START, null, null ).getRowMeta();
        String tableName = KettleEnvPara.ETLWEB_TRANSLOG_TABLE;
        String schemaTable ="";
        String createTable = db.getDDL( tableName, fields );

        if ( !Const.isEmpty( createTable ) ) {
            ddl.append( "-- " ).append( logTable.getLogTableType() ).append( Const.CR );
            ddl.append( "--" ).append( Const.CR ).append( Const.CR );
            ddl.append( createTable ).append( Const.CR );
        }
        return ddl.toString();
    }

    //生成步骤日志表sql语句
    private String getStepCreateTableSQL(Database db) throws  Exception{
        TransMeta transMeta = new TransMeta();
        transMeta.getStepLogTable().findField(StepLogTable.ID.LOG_FIELD.toString()).setEnabled(true);
        StepLogTable logTable = (StepLogTable) transMeta.getStepLogTable().clone();
        StringBuilder ddl = new StringBuilder();

        RowMetaInterface fields = logTable.getLogRecord( LogStatus.START, null, null ).getRowMeta();
        String tableName = KettleEnvPara.ETLWEB_STEPLOG_TABLE;
        String schemaTable ="";
        String createTable = db.getDDL( tableName, fields );

        if ( !Const.isEmpty( createTable ) ) {
            ddl.append( "-- " ).append( logTable.getLogTableType() ).append( Const.CR );
            ddl.append( "--" ).append( Const.CR ).append( Const.CR );
            ddl.append( createTable ).append( Const.CR );
        }
        return ddl.toString();
    }

    //设置仓库连接信息
    public void setKettleRepositoryAccessInfo(String databaseName,String hostname,String port,
                                              String user,String password,String type){
        this.databaseName=databaseName;
        this.hostname=hostname;
        this.port=port;
        this.user=user;
        this.password=password;
        this.type=type;
    }

    //设置仓库登录用户信息
    public void setKettleRepositoryLoginInfo(String loginUser,String loginPass){
        this.etlRepositoryLoginUser=loginUser;
        this.etlRepositoryLoginPass = loginPass;
    }

    /**
     * 方法名:         reLogin
     * 方法功能描述:    重新登录仓库
     * @param
     * @return
     * @Author        SF-2186
     * @Create Date:  2017/5/25  9:05
     */
    public void reLogin() throws Exception{
        RepositoryUtil.login(repositoryMeta,etlRepositoryLoginUser,etlRepositoryLoginPass,true);
    }

    /**
     * 方法名:         reLogin
     * 方法功能描述:    重新登录仓库
     * @param
     * @return
     * @Author        SF-2186
     * @Create Date:  2017/5/25  9:05
     */
    public void logout() throws Exception{
        RepositoryUtil.logout();
    }
}
