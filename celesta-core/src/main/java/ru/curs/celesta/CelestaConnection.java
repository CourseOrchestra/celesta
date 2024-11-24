package ru.curs.celesta;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Adapter class for {@link Connection} where every method call is delegated
 * to the corresponding method of {@code connection} delegate. Child classes
 * can change this behavior.
 */
public abstract class CelestaConnection implements Connection {

  private final Connection connection;

  CelestaConnection(Connection connection) {
    this.connection = connection;
  }

  /**
   * Returns SQL connection object.
   * @return
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement createStatement() throws SQLException {
    return connection.createStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return connection.prepareStatement(sql);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return connection.prepareCall(sql);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String nativeSQL(String sql) throws SQLException {
    return connection.nativeSQL(sql);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    connection.setAutoCommit(autoCommit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean getAutoCommit() throws SQLException {
    return connection.getAutoCommit();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commit() throws SQLException {
    connection.commit();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback() throws SQLException {
    connection.rollback();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return connection.getMetaData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    connection.setReadOnly(readOnly);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCatalog(String catalog) throws SQLException {
    connection.setCatalog(catalog);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCatalog() throws SQLException {
    return connection.getCatalog();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    connection.setTransactionIsolation(level);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getTransactionIsolation() throws SQLException {
    return connection.getTransactionIsolation();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    return connection.getWarnings();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearWarnings() throws SQLException {
    connection.clearWarnings();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return connection.createStatement(resultSetType, resultSetConcurrency);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(
          String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

    return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return connection.getTypeMap();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    connection.setTypeMap(map);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setHoldability(int holdability) throws SQLException {
    connection.setHoldability(holdability);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getHoldability() throws SQLException {
    return connection.getHoldability();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Savepoint setSavepoint() throws SQLException {
    return connection.setSavepoint();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return connection.setSavepoint(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    connection.rollback(savepoint);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    connection.releaseSavepoint(savepoint);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement createStatement(
          int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

    return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(
          String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CallableStatement prepareCall(
          String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

    return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return connection.prepareStatement(sql, autoGeneratedKeys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return connection.prepareStatement(sql, columnIndexes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return connection.prepareStatement(sql, columnNames);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Clob createClob() throws SQLException {
    return connection.createClob();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Blob createBlob() throws SQLException {
    return connection.createBlob();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NClob createNClob() throws SQLException {
    return connection.createNClob();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SQLXML createSQLXML() throws SQLException {
    return connection.createSQLXML();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isValid(int timeout) throws SQLException {
    return connection.isValid(timeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    connection.setClientInfo(name, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    connection.setClientInfo(properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getClientInfo(String name) throws SQLException {
    return connection.getClientInfo(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getClientInfo() throws SQLException {
    return connection.getClientInfo();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return connection.createArrayOf(typeName, elements);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return connection.createStruct(typeName, attributes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSchema(String schema) throws SQLException {
    connection.setSchema(schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchema() throws SQLException {
    return connection.getSchema();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void abort(Executor executor) throws SQLException {
    connection.abort(executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    connection.setNetworkTimeout(executor, milliseconds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getNetworkTimeout() throws SQLException {
    return connection.getNetworkTimeout();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return connection.unwrap(iface);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return connection.isWrapperFor(iface);
  }

  /**
   * Does nothing.
   */
  @Override
  public void close() {
    //Do nothing: to be overriden in pool
    //here we declare it only to eliminate SQLException
  }

}
