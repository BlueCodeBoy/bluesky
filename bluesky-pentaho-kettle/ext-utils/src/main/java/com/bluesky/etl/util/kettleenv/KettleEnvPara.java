package com.bluesky.etl.util.kettleenv;

/**
 * Created by SF-2186 on 2017/3/6.
 */
public class KettleEnvPara {
    /**
     * 数据库连接名称
     */
    public static final String ETLWEB_CONNECT_NAME="etlweb-conn-name";
    /**
     * 资源库连接名称
     */
    public static final String ETLWEB_REPOSITORY_NAME="etlweb-rep-name";

    /**
     * 转换日志表名
     */
    public static final String ETLWEB_TRANSLOG_TABLE="R_TRAN_TRAN_LOG";

    /**
     * 步骤日志表名
     */
    public static final String ETLWEB_STEPLOG_TABLE="R_TRAN_STEP_LOG";

    /**
     * 步骤处理过程数据
     */
    public static final String ETLWEB_TRAN_PROCESS_DATA_TABLE="R_TRAN_PROCESS_DATA";
}
