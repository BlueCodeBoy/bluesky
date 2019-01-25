package com.bluesky.etl.util;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;

/**
 * Created by SF-2186 on 2017/9/15.
 */
public interface TransExecutorListener {
    //行读取事件
    void rowReadEvent(String transName, String stepName, RowMetaInterface rowMeta, Object[] row);
    //行写入事件
    void rowWrittenEvent(String transName, String setpName, RowMetaInterface rowMeta, Object[] row);
    //行错误数据
    void errorRowWrittenEvent(String transName, String setpName, RowMetaInterface rowMetaInterface, Object[] objects);
    //转换启动
    void transStarted(Trans trans);
    //转换激活
    void transActive(Trans trans);
    //转换完成
    void transFinished(Trans trans);
}
