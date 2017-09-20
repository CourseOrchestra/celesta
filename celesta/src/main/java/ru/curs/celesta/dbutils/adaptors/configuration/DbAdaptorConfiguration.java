package ru.curs.celesta.dbutils.adaptors.configuration;


import ru.curs.celesta.AppSettings;

public class DbAdaptorConfiguration {

  private AppSettings.DBType dbType;
  private boolean h2ReferentialIntegrity;

  public AppSettings.DBType getDbType() {
    return dbType;
  }

  public void setDbType(AppSettings.DBType dbType) {
    this.dbType = dbType;
  }

  public boolean isH2ReferentialIntegrity() {
    return h2ReferentialIntegrity;
  }

  public void setH2ReferentialIntegrity(boolean h2ReferentialIntegrity) {
    this.h2ReferentialIntegrity = h2ReferentialIntegrity;
  }
}
