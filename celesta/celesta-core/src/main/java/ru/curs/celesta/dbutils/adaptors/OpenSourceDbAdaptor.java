package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.meta.DBColumnInfo;
import ru.curs.celesta.dbutils.meta.DBIndexInfo;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by ioann on 02.05.2017.
 */
public abstract class OpenSourceDbAdaptor extends DBAdaptor {
  protected static final String CONJUGATE_INDEX_POSTFIX = "__vpo";
  protected static final String SELECT_S_FROM = "select %s from ";
  protected static final String NOW = "now()";
  protected static final Pattern DATEPATTERN = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");

  protected static final Pattern QUOTED_NAME = Pattern.compile("\"?([^\"]+)\"?");

  public OpenSourceDbAdaptor(ConnectionPool connectionPool) {
    super(connectionPool);
  }

  @Override
	public boolean tableExists(Connection conn, String schema, String name) throws CelestaException {
		try (PreparedStatement check = conn
				.prepareStatement(String.format("SELECT table_name FROM information_schema.tables  WHERE "
						+ "table_schema = '%s' AND table_name = '%s'", schema, name))) {
			ResultSet rs = check.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}
	
  @Override
  public void dropTrigger(Connection conn, TriggerQuery query) throws SQLException {
    Statement stmt = conn.createStatement();

    try {
      String sql = String.format("DROP TRIGGER \"%s\" ON " + tableTemplate(),
          query.getName(), query.getSchema(), query.getTableName());
      stmt.executeUpdate(sql);
    } finally {
      stmt.close();
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
  public PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) throws CelestaException {
    TableElement t = c.getParentTable();
    String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s limit 1;", c.getQuotedName(),
        t.getGrain().getName(), t.getName(), where);
    return prepareStatement(conn, sql);
  }

  @Override
  public PreparedStatement getOneRecordStatement(
      Connection conn, TableElement t, String where, Set<String> fields
  ) throws CelestaException {

    final String fieldList = getTableFieldsListExceptBlobs((GrainElement) t, fields);
    String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s limit 1;",
        fieldList, t.getGrain().getName(), t.getName(), where);

    PreparedStatement result = prepareStatement(conn, sql);
    //System.out.println(result.toString());
    return result;
  }

  @Override
  public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) throws CelestaException {
    String sql = String.format("delete from " + tableTemplate() + " where %s;", t.getGrain().getName(), t.getName(),
        where);
    return prepareStatement(conn, sql);
  }

  @Override
  public Set<String> getColumns(Connection conn, TableElement t) throws CelestaException {
    String sql = String.format("select column_name from information_schema.columns "
        + "where table_schema = '%s' and table_name = '%s';", t.getGrain().getName(), t.getName());
    return sqlToStringSet(conn, sql);
  }

  @Override
  public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) throws CelestaException {
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
  String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
    String sql = String.format("DROP INDEX IF EXISTS " + tableTemplate(), g.getName(), dBIndexInfo.getIndexName());
    String sql2 = String.format("DROP INDEX IF EXISTS " + tableTemplate(), g.getName(),
        dBIndexInfo.getIndexName() + CONJUGATE_INDEX_POSTFIX);
    String[] result = {sql, sql2};
    return result;
  }


  @Override
  public void updateColumn(Connection conn, Column c, DBColumnInfo actual) throws CelestaException {
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
  void dropAutoIncrement(Connection conn, TableElement t) throws SQLException {
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
  public void createPK(Connection conn, TableElement t) throws CelestaException {
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
  public PreparedStatement getNavigationStatement(
      Connection conn, FromClause from, String orderBy,
      String navigationWhereClause, Set<String> fields, long offset
  ) throws CelestaException {
    if (navigationWhereClause == null)
      throw new IllegalArgumentException();
    StringBuilder w = new StringBuilder(navigationWhereClause);
    final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);
    boolean useWhere = w.length() > 0;
    if (orderBy.length() > 0)
      w.append(" order by " + orderBy);
    String sql = String.format(SELECT_S_FROM + " %s %s  limit 1 offset %d;", fieldList,
        from.getExpression(), useWhere ? " where " + w : w, offset == 0 ? 0 : offset - 1);
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
