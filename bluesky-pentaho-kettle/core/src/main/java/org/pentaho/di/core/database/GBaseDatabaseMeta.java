
package org.pentaho.di.core.database;


public class GBaseDatabaseMeta extends MySQLDatabaseMeta implements DatabaseInterface {
  @Override
  public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override
  public int getDefaultDatabasePort() {
    return 5258;
  }

  @Override
  public String getDriverClass() {
    return "com.gbase.jdbc.Driver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    return "jdbc:gbase://" + hostname + ":" + port + "/" + databaseName;
  }

  @Override
  public boolean supportsCatalogs() {
    return false;
  }

}


