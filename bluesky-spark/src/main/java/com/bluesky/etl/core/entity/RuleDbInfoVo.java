package com.bluesky.etl.core.entity;

/**
 * Created by SF-2186 on 2017/12/21.
 */
public class RuleDbInfoVo extends RuleDbInfo {
    //数据库类型
    String dbType;
    //数据库名
    String dbName;
    //数据库描述
    String dbDesc;
    //数据源名称
    String datasourceName;
    //ip地址
    String hostName;
    //端口
    String dbPort;
    //用户名
    String userName;
    //密码
    String passWord;
    //模式
    String schema;
    //表个数
    int tableSize;
    //任务个数
    int transSize;
    //规则变更通知（YES:有变化   NO:无变化）
    private String ruleChangeNotice;

    /**
     * 校验表与规则对应关系（YES:有   NO:无）)
     */
    private String existRule;

    /**
     * 数据库综合信息(联合查询使用)
     */
    private String dbNameInfo;

    public String getCreateUserName() {
        return createUserName;
    }

    public void setCreateUserName(String createUserName) {
        this.createUserName = createUserName;
    }

    //创建用户名
    String createUserName;

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbDesc() {
        return dbDesc;
    }

    public void setDbDesc(String dbDesc) {
        this.dbDesc = dbDesc;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getDbPort() {
        return dbPort;
    }

    public void setDbPort(String dbPort) {
        this.dbPort = dbPort;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }

    public int getTableSize() {
        return tableSize;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public int getTransSize() {
        return transSize;
    }

    public void setTransSize(int transSize) {
        this.transSize = transSize;
    }


    public String getDatasourceName() {
        return datasourceName;
    }

    public void setDatasourceName(String datasourceName) {
        this.datasourceName = datasourceName;
    }
    public String getRuleChangeNotice() {
        return ruleChangeNotice;
    }

    public void setRuleChangeNotice(String ruleChangeNotice) {
        this.ruleChangeNotice = ruleChangeNotice;
    }

    public String getDbNameInfo() {
        return dbNameInfo;
    }

    public void setDbNameInfo(String dbNameInfo) {
        this.dbNameInfo = dbNameInfo;
    }
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
    public String getExistRule() {
        return existRule;
    }

    public void setExistRule(String existRule) {
        this.existRule = existRule;
    }


    @Override
    public String toString() {
        return "RuleDbInfoVo{" +
                "dbType='" + dbType + '\'' +
                ", dbName='" + dbName + '\'' +
                ", dbDesc='" + dbDesc + '\'' +
                ", hostName='" + hostName + '\'' +
                ", dbPort='" + dbPort + '\'' +
                ", userName='" + userName + '\'' +
                ", passWord='" + passWord + '\'' +
                ", tableSize=" + tableSize +
                ", transSize=" + transSize +
                '}';
    }

}
