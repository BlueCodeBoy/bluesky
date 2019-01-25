package con.bluesky.etl.kettle;

import java.util.TreeMap;

/**
 * Created by root on 18-4-19.
 */
public class kettleutiletest {
    public static void main(String args[]) throws Exception {
        TreeMap a = new TreeMap();
        a.put(1,"1");
        a.put(2,"3");
        a.put(3,"4");
        a.remove(1);
        System.out.println("sss");
        //ETL配置组装
//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("databaseName", "ETL_WEB_BACK");
//        jsonObject.put("hostname", "10.0.8.202");
//        jsonObject.put("port", "3306");
//        jsonObject.put("user", "root");
//        jsonObject.put("password", "123456");
//        jsonObject.put("type", "MYSQL");
//        jsonObject.put("etlRepositoryLoginUser", "admin");
//        jsonObject.put("etlRepositoryLoginPass", "admin");
//        jsonObject.put("tranceName", "test_bigdata");
//        TransMeta transMeta = KettleUtil.getTransMetaByaesString(jsonObject);
//        System.out.println(transMeta.getName());

    }
}
