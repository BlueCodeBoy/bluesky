package org.pentaho.di.core;

import java.util.HashMap;
import java.util.Map;

/**
 * ftp断点信息列表信息
 * Created by liliujiang on 2016/8/3.
 */
public class FtpBreakInfoList {


    //ftp上传断点信息
    private Map<String, FtpBreakInfo> ftpUpBreakInfo = new HashMap<String, FtpBreakInfo>();
    //ftp下载断点信息
    private Map<String, FtpBreakInfo> ftpDownBreakInfo = new HashMap<String, FtpBreakInfo>();
    //sftp上传断点信息
    private Map<String, FtpBreakInfo> sftpUpBreakInfo = new HashMap<String, FtpBreakInfo>();
    //sftp下载断点信息
    private Map<String, FtpBreakInfo> sftpDownBreakInfo = new HashMap<String, FtpBreakInfo>();
    //ftps上传断点信息
    private Map<String, FtpBreakInfo> ftpsUpBreakInfo = new HashMap<String, FtpBreakInfo>();
    //ftps下载断点信息
    private Map<String, FtpBreakInfo> ftpsDownBreakInfo = new HashMap<String, FtpBreakInfo>();

    /*Ftp*/
    public Map<String, FtpBreakInfo> getAllFtpUpBreakInfo(){return ftpUpBreakInfo;}
    public FtpBreakInfo findFtpUpBreakInfoByKey(String jobName)
    {
        return ftpDownBreakInfo.get(jobName);
    }
    public void addFtpUpBreakInfo(String jobName, FtpBreakInfo info) {
        ftpUpBreakInfo.put(jobName, info);
    }
    public void removeFtpUpBreakInfo(String jobName) {
        ftpUpBreakInfo.remove(jobName);
    }


    public Map<String, FtpBreakInfo> getAllFtpDownBreakInfo(){return ftpDownBreakInfo;}
    public FtpBreakInfo findFtpDownBreakInfoByKey(String jobName)
    {
        return ftpDownBreakInfo.get(jobName);
    }
    public void addFtpDownBreakInfo(String jobName, FtpBreakInfo info) {
        ftpDownBreakInfo.put(jobName,info);
    }
    public void removeFtpDownBreakInfo(String jobName) {
        ftpDownBreakInfo.remove(jobName);
    }
    /*Ftp结束*/

    /*SFtp*/
    public Map<String, FtpBreakInfo> getAllSFtpUpBreakInfo(){return sftpUpBreakInfo;}
    public FtpBreakInfo findSFtpUpBreakInfoByKey(String jobName)
    {
        return sftpUpBreakInfo.get(jobName);
    }
    public void addSFtpUpBreakInfo(String jobName, FtpBreakInfo info) {
        sftpUpBreakInfo.put(jobName, info);
    }
    public void removeSFtpUpBreakInfo(String jobName) {
        sftpUpBreakInfo.remove(jobName);
    }
    public Map<String, FtpBreakInfo> getAllSFtpDownBreakInfo(){return sftpDownBreakInfo;}
    public FtpBreakInfo findSFtpDownBreakInfoByKey(String jobName)
    {
        return sftpDownBreakInfo.get(jobName);
    }
    public void addSFtpDownBreakInfo(String jobName, FtpBreakInfo info) {
        sftpDownBreakInfo.put(jobName, info);
    }
    public void removeSFtpDownBreakInfo(String jobName) {
        sftpDownBreakInfo.remove(jobName);
    }
    /*SFtp结束*/

    /*Ftps*/
    public Map<String, FtpBreakInfo> getAllFtpsUpBreakInfo(){return ftpsUpBreakInfo;}
    public FtpBreakInfo findFtpsUpBreakInfoByKey(String jobName)
    {
        return ftpsDownBreakInfo.get(jobName);
    }
    public void addFtpsUpBreakInfo(String jobName, FtpBreakInfo info) {
        ftpsUpBreakInfo.put(jobName, info);
    }
    public void removeFtpsUpBreakInfo(String jobName) {
        ftpsUpBreakInfo.remove(jobName);
    }

    public Map<String, FtpBreakInfo> getAllFtpsDownBreakInfo(){return ftpsDownBreakInfo;}
    public FtpBreakInfo findFtpsDownBreakInfoByKey(String jobName)
    {
        return ftpsDownBreakInfo.get(jobName);
    }
    public void addFtpsDownBreakInfo(String jobName, FtpBreakInfo info) {
        ftpsDownBreakInfo.put(jobName, info);
    }
    public void removeFtpsDownBreakInfo(String jobName) {
        ftpsDownBreakInfo.remove(jobName);
    }
    /*Ftps结束*/

    //ftp断点信息
    public class FtpBreakInfo {
        //数据源ip地址
        private String srcIp;
        //文件全路径
        private String fileFullName;
        //断点位置
        private Integer breakPos;

        public FtpBreakInfo(String srcIp,String fileFullName,Integer breakPos)
        {
            this.srcIp=srcIp;
            this.fileFullName=fileFullName;
            this.breakPos=breakPos;
        }

        public String getSrcIp() {
            return srcIp;
        }

        public void setSrcIp(String srcIp) {
            this.srcIp = srcIp;
        }

        public String getFileFullName() {
            return srcIp;
        }

        public void setFileFullName(String fileFullName) {
            this.srcIp = srcIp;
        }

        public Integer getBreakPos() {
            return breakPos;
        }

        public void setBreakPos(Integer breakPos) {
            this.breakPos = breakPos;
        }
    }
}




