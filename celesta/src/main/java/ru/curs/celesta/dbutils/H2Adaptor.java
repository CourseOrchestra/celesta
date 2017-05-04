package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.util.Map;


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
    return null;
  }

  @Override
  Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException {
    return null;
  }

  @Override
  public int getDBPid(Connection conn) throws CelestaException {
    return 0;
  }
}
