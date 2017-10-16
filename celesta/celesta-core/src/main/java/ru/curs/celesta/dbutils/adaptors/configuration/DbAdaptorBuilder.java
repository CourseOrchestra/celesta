package ru.curs.celesta.dbutils.adaptors.configuration;


import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

public class DbAdaptorBuilder {

  private AppSettings.DBType dbType;
  private ConnectionPool connectionPool;
  private boolean h2ReferentialIntegrity;

  public DbAdaptorBuilder setDbType(AppSettings.DBType dbType) {
    this.dbType = dbType;
    return this;
  }

  public DbAdaptorBuilder setConnectionPool(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
    return this;
  }

  public DbAdaptorBuilder setH2ReferentialIntegrity(boolean h2ReferentialIntegrity) {
    this.h2ReferentialIntegrity = h2ReferentialIntegrity;
    return this;
  }

  public DBAdaptor createDbAdaptor() throws CelestaException {
    return DBAdaptor.create(dbType, connectionPool, h2ReferentialIntegrity);
  }
}
