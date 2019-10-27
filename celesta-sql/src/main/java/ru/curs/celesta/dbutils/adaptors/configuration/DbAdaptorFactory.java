package ru.curs.celesta.dbutils.adaptors.configuration;


import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.FirebirdAdaptor;
import ru.curs.celesta.dbutils.adaptors.H2Adaptor;
import ru.curs.celesta.dbutils.adaptors.MSSQLAdaptor;
import ru.curs.celesta.dbutils.adaptors.OraAdaptor;
import ru.curs.celesta.dbutils.adaptors.PostgresAdaptor;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;

/**
 * DB adaptor builder.<br/>
 * <br/>
 * Depending on DB type the following descendants of {@link DBAdaptor} are returned:
 * <ul>
 *   <li><b>H2</b> - {@link H2Adaptor}</li>
 *   <li><b>Postgre SQL</b> - {@link PostgresAdaptor}</li>
 *   <li><b>MS SQL</b> - {@link MSSQLAdaptor}</li>
 *   <li><b>Oracle</b> - {@link OraAdaptor}</li>
 * </ul>
 */
public final class DbAdaptorFactory {

  private DBType dbType;
  private ConnectionPool connectionPool;
  private DdlConsumer ddlConsumer;
  private boolean h2ReferentialIntegrity;

  /**
   * Sets DB type.
   *
   * @param dbType DB type
   * @return  {@code this}
   */
  public DbAdaptorFactory setDbType(DBType dbType) {
    this.dbType = dbType;
    return this;
  }

  /**
   * Sets DDL Consumer.
   *
   * @param ddlConsumer  DDL consumer
   * @return  @return  {@code this}
   */
  public DbAdaptorFactory setDdlConsumer(DdlConsumer ddlConsumer) {
    this.ddlConsumer = ddlConsumer;
    return this;
  }

  /**
   * Sets Connection pool.
   *
   * @param connectionPool  connection pool
   * @return  @return  {@code this}
   */
  public DbAdaptorFactory setConnectionPool(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
    return this;
  }

  /**
   * Whether referential integrity for H2 DB has to be switched on/off.
   *
   * @param h2ReferentialIntegrity  {@code true} - switch on, {@code false} - switch off.
   *        Defaul value is {@code false}.
   * @return
   */
  public DbAdaptorFactory setH2ReferentialIntegrity(boolean h2ReferentialIntegrity) {
    this.h2ReferentialIntegrity = h2ReferentialIntegrity;
    return this;
  }

  /**
   * Builds DB adaptor for concrete DB type.
   *
   * @return
   */
  public DBAdaptor createDbAdaptor() {

    if (DBType.H2.equals(dbType)) {
      return new H2Adaptor(this.connectionPool, this.ddlConsumer, this.h2ReferentialIntegrity);
    }
    if (DBType.POSTGRESQL.equals(dbType)) {
      return new PostgresAdaptor(this.connectionPool, this.ddlConsumer);
    }
    if (DBType.MSSQL.equals(dbType)) {
      return new MSSQLAdaptor(this.connectionPool, this.ddlConsumer);
    }
    if (DBType.ORACLE.equals(dbType)) {
      return new OraAdaptor(this.connectionPool, this.ddlConsumer);
    }
    if (DBType.FIREBIRD.equals(dbType)) {
      return new FirebirdAdaptor(this.connectionPool, this.ddlConsumer);
    }

    return null;
  }

}
