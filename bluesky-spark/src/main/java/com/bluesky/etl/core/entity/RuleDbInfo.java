/********************************************************************************
** 作者： 
** 创始时间：
** 修改人：
** 修改时间：
** 修改人：
** 修改时间：
** 描述：校验库信息
**    
*********************************************************************************/
package com.bluesky.etl.core.entity;

import java.io.Serializable;
import java.sql.Timestamp;

public class RuleDbInfo implements Serializable{
    /**
     * 流水号
     */  
    private String flowId;
    
    /**
     * 元数据中的db唯一码
     */  
    private String dbMetadataUniqueCode;

    /**
     * 扩展属性
     */
    private String properties;
    
    /**
     * 创建人
     */  
    private String createUser;
    
    /**
     * 创建时间
     */  
    private Timestamp createTime;

    /**
     * 设计无权限用户(逗号分隔)
     */
    private String  designNoAuthUser;
    

    public String getFlowId() {
        return flowId;
    }
    
    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }
    public String getDbMetadataUniqueCode() {
        return dbMetadataUniqueCode;
    }
    
    public void setDbMetadataUniqueCode(String dbMetadataUniqueCode) {
        this.dbMetadataUniqueCode = dbMetadataUniqueCode;
    }
    public String getCreateUser() {
        return createUser;
    }
    
    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }
    public Timestamp getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public String getDesignNoAuthUser() {
        return designNoAuthUser;
    }

    public void setDesignNoAuthUser(String designNoAuthUser) {
        this.designNoAuthUser = designNoAuthUser;
    }

}
