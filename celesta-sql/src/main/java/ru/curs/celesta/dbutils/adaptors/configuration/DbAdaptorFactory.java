package ru.curs.celesta.dbutils.adaptors.configuration;


import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.*;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;

public class DbAdaptorFactory {

  private DBType dbType;
  private ConnectionPool connectionPool;
  private DdlConsumer ddlConsumer;
  private boolean h2ReferentialIntegrity;

  public DbAdaptorFactory setDbType(DBType dbType) {
    this.dbType = dbType;
    return this;
  }

  public DbAdaptorFactory setDdlConsumer(DdlConsumer ddlConsumer) {
    this.ddlConsumer = ddlConsumer;
    return this;
  }

  public DbAdaptorFactory setConnectionPool(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
    return this;
  }

  public DbAdaptorFactory setH2ReferentialIntegrity(boolean h2ReferentialIntegrity) {
    this.h2ReferentialIntegrity = h2ReferentialIntegrity;
    return this;
  }

  public DBAdaptor createDbAdaptor() {

    if (DBType.H2.equals(dbType))
      return new H2Adaptor(this.connectionPool, this.ddlConsumer, this.h2ReferentialIntegrity);
    if (DBType.POSTGRESQL.equals(dbType))
      return new PostgresAdaptor(this.connectionPool, this.ddlConsumer);
    if (DBType.MSSQL.equals(dbType))
      return new MSSQLAdaptor(this.connectionPool, this.ddlConsumer);
    if (DBType.ORACLE.equals(dbType))
      return new OraAdaptor(this.connectionPool, this.ddlConsumer);

    return null;
  }


}
