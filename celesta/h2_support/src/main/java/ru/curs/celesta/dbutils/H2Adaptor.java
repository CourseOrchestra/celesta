package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.h2.RecVersionCheckTrigger;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;


/**
 * Created by ioann on 02.05.2017.
 */
final public class H2Adaptor extends SqlDbAdaptor {


  @Override
  boolean userTablesExist(Connection conn) throws SQLException {
    PreparedStatement check = conn
        .prepareStatement("select count(*) from information_schema.tables " +
            "WHERE table_type = 'TABLE' " +
            "AND table_schema <> 'INFORMATION_SCHEMA';");
    ResultSet rs = check.executeQuery();

    try {
      rs.next();
      return rs.getInt(1) != 0;
    } finally {
      check.close();
      rs.close();
    }
  }

  @Override
  void manageAutoIncrement(Connection conn, Table t) throws SQLException {
    String sql;
    Statement stmt = conn.createStatement();
    try {
      // 1. Firstly, we have to clean up table from any auto-increment
      // defaults. Meanwhile we check if table has IDENTITY field, if it
      // doesn't, no need to proceed.
      IntegerColumn idColumn = null;
      for (Column c : t.getColumns().values())
        if (c instanceof IntegerColumn) {
          IntegerColumn ic = (IntegerColumn) c;
          if (ic.isIdentity())
            idColumn = ic;
          else {
            if (ic.getDefaultValue() == null) {
              sql = String.format("alter table %s.%s alter column %s drop default",
                  t.getGrain().getQuotedName(), t.getQuotedName(), ic.getQuotedName());
            } else {
              sql = String.format("alter table %s.%s alter column %s set default %d",
                  t.getGrain().getQuotedName(), t.getQuotedName(), ic.getQuotedName(),
                  ic.getDefaultValue().intValue());
            }
            stmt.executeUpdate(sql);
          }
        }

      if (idColumn == null)
        return;

      // 2. Now, we know that we surely have IDENTITY field, and we have
      // to be sure that we have an appropriate sequence.
      boolean hasSequence = false;
      sql = String.format(
          "SELECT COUNT(*) FROM information_schema.sequences " +
              "WHERE sequence_schema = '%s' " +
              "AND sequence_name = '%s_seq'",
          t.getGrain().getName(), t.getName());
      ResultSet rs = stmt.executeQuery(sql);
      rs.next();
      try {
        hasSequence = rs.getInt(1) > 0;
      } finally {
        rs.close();
      }
      if (!hasSequence) {
        sql = String.format("create sequence \"%s\".\"%s_seq\" increment 1 minvalue 1", t.getGrain().getName(),
            t.getName());
        stmt.executeUpdate(sql);
      }

      // 3. Now we have to create the auto-increment default
      sql = String.format(
          "alter table %s.%s alter column %s set default " + "nextval('\"%s\".\"%s_seq\"');",
          t.getGrain().getQuotedName(), t.getQuotedName(), idColumn.getQuotedName(), t.getGrain().getName(),
          t.getName());
      stmt.executeUpdate(sql);
    } finally {
      stmt.close();
    }
  }

  @Override
  DBPKInfo getPKInfo(Connection conn, Table t) throws CelestaException {
    String sql = String.format(
        "SELECT index_name AS indexName, column_name as colName " +
            "FROM  INFORMATION_SCHEMA.INDEXES " +
            "WHERE table_schema = '%s' " +
            "AND table_name = '%s' " +
            "AND index_type_name = 'PRIMARY KEY'",
        t.getGrain().getName(), t.getName());
    DBPKInfo result = new DBPKInfo();

    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(sql);

        while (rs.next()) {
          if (result.getName() == null) {
            String indName = rs.getString("indexName");
            result.setName(indName);
          }

          String colName = rs.getString("colName");
          result.getColumnNames().add(colName);
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Could not get indices information: %s", e.getMessage());
    }
    return result;
  }


  @Override
  Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException {
    Map<String, DBIndexInfo> result = new HashMap<>();

    String sql = String.format(
        "SELECT table_name as tableName, index_name as indexName, column_name as colName " +
            "FROM  INFORMATION_SCHEMA.INDEXES " +
            "WHERE table_schema = '%s'",
        g.getName());

    try {
      Statement stmt = conn.createStatement();

      try {
        ResultSet rs = stmt.executeQuery(sql);

        while (rs.next()) {
          String indexName = rs.getString("indexName");
          DBIndexInfo ii = result.get(indexName);

          if (ii == null) {
            String tableName = rs.getString("tableName");
            ii = new DBIndexInfo(tableName, indexName);
            result.put(indexName, ii);
          }

          String colName = rs.getString("colName");
          ii.getColumnNames().add(colName);
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Could not get indices information: %s", e.getMessage());
    }

    return result;
  }

  @Override
  public void updateVersioningTrigger(Connection conn, Table t) throws CelestaException {
    // First of all, we are about to check if trigger exists
    String sql = String.format("select count(*) from information_schema.triggers where "
        + "		table_schema = '%s' and table_name = '%s'"
        + "		and trigger_name = 'versioncheck'", t.getGrain().getName(), t.getName());
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        boolean triggerExists = rs.getInt(1) > 0;
        rs.close();
        if (t.isVersioned()) {
          if (triggerExists) {
            return;
          } else {
            // CREATE TRIGGER
            sql = String.format(
                "CREATE TRIGGER \"versioncheck\"" + " BEFORE UPDATE ON " + tableTemplate()
                    + " FOR EACH ROW CALL \"%s\"",
                t.getGrain().getName(), t.getName(), RecVersionCheckTrigger.class.getName());
            stmt.executeUpdate(sql);
          }
        } else {
          if (triggerExists) {
            // DROP TRIGGER
            sql = String.format("DROP TRIGGER \"versioncheck\" ON " + tableTemplate(),
                t.getGrain().getName(), t.getName());
            stmt.executeUpdate(sql);
          } else {
            return;
          }
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Could not update version check trigger on %s.%s: %s", t.getGrain().getName(),
          t.getName(), e.getMessage());
    }

  }

  @Override
  public int getDBPid(Connection conn) throws CelestaException {
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT SESSION_ID()");
        if (rs.next()) {
          return rs.getInt(1);
        } else {
          return 0;
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

}
