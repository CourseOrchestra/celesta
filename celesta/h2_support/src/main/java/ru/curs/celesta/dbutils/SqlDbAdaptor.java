package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ioann on 02.05.2017.
 */
public abstract class SqlDbAdaptor extends DBAdaptor {
  protected static final String CONJUGATE_INDEX_POSTFIX = "__vpo";
  protected static final String SELECT_S_FROM = "select %s from ";
  protected static final String NOW = "now()";
  protected static final Pattern POSTGRESDATEPATTERN = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");

  protected static final Pattern QUOTED_NAME = Pattern.compile("\"?([^\"]+)\"?");

  protected static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();


  @Override
  boolean tableExists(Connection conn, String schema, String name) throws CelestaException {
    try {
      PreparedStatement check = conn
          .prepareStatement(String.format("SELECT table_name FROM information_schema.tables  WHERE "
              + "table_schema = '%s' AND table_name = '%s'", schema, name));
      ResultSet rs = check.executeQuery();
      try {
        return rs.next();
      } finally {
        rs.close();
        check.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  @Override
  void createSchemaIfNotExists(Connection conn, String name) throws SQLException {
    String sql = String.format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s';",
        name);
    Statement check = conn.createStatement();
    ResultSet rs = check.executeQuery(sql);
    try {
      if (!rs.next()) {
        check.executeUpdate(String.format("create schema \"%s\";", name));
      }
    } finally {
      rs.close();
      check.close();
    }
  }

  @Override
  ColumnDefiner getColumnDefiner(Column c) {
    return TYPES_DICT.get(c.getClass());
  }

  @Override
  PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) throws CelestaException {
    Table t = c.getParentTable();
    String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s limit 1;", c.getQuotedName(),
        t.getGrain().getName(), t.getName(), where);
    return prepareStatement(conn, sql);
  }

  @Override
  PreparedStatement getOneRecordStatement(Connection conn, Table t, String where) throws CelestaException {
    String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s limit 1;",
        getTableFieldsListExceptBLOBs(t), t.getGrain().getName(), t.getName(), where);
    return prepareStatement(conn, sql);
  }

  @Override
  PreparedStatement getDeleteRecordStatement(Connection conn, Table t, String where) throws CelestaException {
    String sql = String.format("delete from " + tableTemplate() + " where %s;", t.getGrain().getName(), t.getName(),
        where);
    return prepareStatement(conn, sql);
  }

  @Override
  public Set<String> getColumns(Connection conn, Table t) throws CelestaException {
    String sql = String.format("select column_name from information_schema.columns "
        + "where table_schema = '%s' and table_name = '%s';", t.getGrain().getName(), t.getName());
    return sqlToStringSet(conn, sql);
  }

  @Override
  PreparedStatement deleteRecordSetStatement(Connection conn, Table t, String where) throws CelestaException {
    // Готовим запрос на удаление
    String sql = String.format("delete from " + tableTemplate() + " %s;", t.getGrain().getName(), t.getName(),
        where.isEmpty() ? "" : "where " + where);
    try {
      PreparedStatement result = conn.prepareStatement(sql);
      return result;
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  @Override
  String[] getCreateIndexSQL(Index index) {

    StringBuilder sb = new StringBuilder();
    StringBuilder sb2 = new StringBuilder();
    boolean conjugate = false;
    for (Map.Entry<String, Column> c : index.getColumns().entrySet()) {
      if (sb.length() > 0) {
        sb.append(", ");
        sb2.append(", ");
      }
      sb.append('"');
      sb2.append('"');
      sb.append(c.getKey());
      sb2.append(c.getKey());
      sb.append('"');
      sb2.append('"');

      if (c.getValue() instanceof StringColumn && !((StringColumn) c.getValue()).isMax()) {
        sb2.append(" varchar_pattern_ops");
        conjugate = true;
      }
    }

    String sql = String.format("CREATE INDEX \"%s\" ON " + tableTemplate() + " (%s)", index.getName(),
        index.getTable().getGrain().getName(), index.getTable().getName(), sb.toString());
    if (conjugate) {
      String sql2 = String.format("CREATE INDEX \"%s\" ON " + tableTemplate() + " (%s)",
          index.getName() + CONJUGATE_INDEX_POSTFIX, index.getTable().getGrain().getName(),
          index.getTable().getName(), sb2.toString());
      String[] result = { sql, sql2 };
      return result;
    } else {
      String[] result = { sql };
      return result;
    }

  }

  @Override
  String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
    String sql = String.format("DROP INDEX " + tableTemplate(), g.getName(), dBIndexInfo.getIndexName());
    String sql2 = String.format("DROP INDEX IF EXISTS " + tableTemplate(), g.getName(),
        dBIndexInfo.getIndexName() + CONJUGATE_INDEX_POSTFIX);
    String[] result = { sql, sql2 };
    return result;
  }


  @Override
  void updateColumn(Connection conn, Column c, DBColumnInfo actual) throws CelestaException {
    try {
      String sql;
      List<String> batch = new LinkedList<>();
      // Начинаем с удаления default-значения
      sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" DROP DEFAULT",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName());
      batch.add(sql);

      updateColType(c, actual, batch);

      // Проверяем nullability
      if (c.isNullable() != actual.isNullable()) {
        sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" %s",
            c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName(),
            c.isNullable() ? "DROP NOT NULL" : "SET NOT NULL");
        batch.add(sql);
      }

      // Если в данных пустой default, а в метаданных -- не пустой -- то
      if (c.getDefaultValue() != null || (c instanceof DateTimeColumn && ((DateTimeColumn) c).isGetdate())) {
        sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" SET %s",
            c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName(),
            getColumnDefiner(c).getDefaultDefinition(c));
        batch.add(sql);
      }

      Statement stmt = conn.createStatement();
      try {
        // System.out.println(">>batch begin>>");
        for (String s : batch) {
          // System.out.println(s);
          stmt.executeUpdate(s);
        }
        // System.out.println("<<batch end<<");
      } finally {
        stmt.close();
      }

    } catch (SQLException e) {
      throw new CelestaException("Cannot modify column %s on table %s.%s: %s", c.getName(),
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), e.getMessage());

    }

  }

  abstract protected void updateColType(Column c, DBColumnInfo actual, List<String> batch);

  @Override
  void dropAutoIncrement(Connection conn, Table t) throws SQLException {
    // Удаление Sequence
    String sql = String.format("drop sequence if exists \"%s\".\"%s_seq\"", t.getGrain().getName(), t.getName());
    Statement stmt = conn.createStatement();
    try {
      stmt.execute(sql);
    } finally {
      stmt.close();
    }
  }


  @Override
  void dropPK(Connection conn, Table t, String pkName) throws CelestaException {
    String sql = String.format("alter table %s.%s drop constraint \"%s\" cascade", t.getGrain().getQuotedName(),
        t.getQuotedName(), pkName);
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Cannot drop PK '%s': %s", pkName, e.getMessage());
    }
  }

  @Override
  void createPK(Connection conn, Table t) throws CelestaException {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("alter table %s.%s add constraint \"%s\" primary key (", t.getGrain().getQuotedName(),
        t.getQuotedName(), t.getPkConstraintName()));
    boolean multiple = false;
    for (String s : t.getPrimaryKey().keySet()) {
      if (multiple)
        sql.append(", ");
      sql.append('"');
      sql.append(s);
      sql.append('"');
      multiple = true;
    }
    sql.append(")");

    // System.out.println(sql.toString());
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql.toString());
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Cannot create PK '%s': %s", t.getPkConstraintName(), e.getMessage());
    }
  }


  @Override
  String getLimitedSQL(GrainElement t, String whereClause, String orderBy, long offset, long rowCount) {
    if (offset == 0 && rowCount == 0)
      throw new IllegalArgumentException();
    String sql;
    if (offset == 0)
      sql = getSelectFromOrderBy(t, whereClause, orderBy) + String.format(" limit %d", rowCount);
    else if (rowCount == 0)
      sql = getSelectFromOrderBy(t, whereClause, orderBy) + String.format(" limit all offset %d", offset);
    else {
      sql = getSelectFromOrderBy(t, whereClause, orderBy)
          + String.format(" limit %d offset %d", rowCount, offset);
    }
    return sql;
  }


  @Override
  public SQLGenerator getViewSQLGenerator() {
    return new SQLGenerator();
  }

  @Override
  PreparedStatement getNavigationStatement(Connection conn, GrainElement t, String orderBy,
                                           String navigationWhereClause) throws CelestaException {
    if (navigationWhereClause == null)
      throw new IllegalArgumentException();
    StringBuilder w = new StringBuilder(navigationWhereClause);
    boolean useWhere = w.length() > 0;
    if (orderBy.length() > 0)
      w.append(" order by " + orderBy);
    String sql = String.format(SELECT_S_FROM + tableTemplate() + "%s  limit 1;", getTableFieldsListExceptBLOBs(t),
        t.getGrain().getName(), t.getName(), useWhere ? " where " + w : w);
    // System.out.println(sql);
    return prepareStatement(conn, sql);
  }

  @Override
  public void resetIdentity(Connection conn, Table t, int i) throws SQLException {
    Statement stmt = conn.createStatement();
    try {
      String sql = String.format("alter sequence \"%s\".\"%s_seq\" restart with %d", t.getGrain().getName(),
          t.getName(), i);
      stmt.executeUpdate(sql);
    } finally {
      stmt.close();
    }
  }

  @Override
  public boolean nullsFirst() {
    return false;
  }

}
