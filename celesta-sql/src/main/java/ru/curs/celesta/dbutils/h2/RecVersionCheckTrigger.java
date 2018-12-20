package ru.curs.celesta.dbutils.h2;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by ioann on 03.05.2017.
 */
public class RecVersionCheckTrigger implements Trigger {

  int indexOfRecVersionColumn;

  @Override
  public void init(Connection connection, String schemaName, String triggerName, String tableName,
                   boolean before, int type) throws SQLException {
    String sql = "SELECT ordinal_position - 1 "
               + "FROM information_schema.columns "
               + "WHERE table_schema = '%s' "
                   + "AND table_name = '%s' "
                   + "AND column_name = 'recversion'";
    sql = String.format(sql, schemaName, tableName);

    Statement statement = connection.createStatement();

    try {
      ResultSet resultSet = statement.executeQuery(sql);

      if (resultSet.next()) {
        indexOfRecVersionColumn = resultSet.getInt(1);
      } else {
        throw new SQLException(String.format("Can't find recversion column for %s.%s", schemaName, tableName));
      }
    } finally {
      statement.close();
    }
  }

  @Override
  public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
    Integer oldRecVersion = (Integer) oldRow[indexOfRecVersionColumn];
    Integer newRecVersion = (Integer) newRow[indexOfRecVersionColumn];

    if (oldRecVersion.equals(newRecVersion)) {
      newRow[indexOfRecVersionColumn] = ++newRecVersion;
    } else {
      throw new SQLException("record version check failure");
    }
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public void remove() throws SQLException {

  }
}
