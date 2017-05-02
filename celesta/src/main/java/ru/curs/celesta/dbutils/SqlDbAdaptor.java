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
  protected static final Pattern HEX_STRING = Pattern.compile("'\\\\x([0-9A-Fa-f]+)'");

  protected static final Pattern QUOTED_NAME = Pattern.compile("\"?([^\"]+)\"?");

  protected static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();

  static {
    TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
      @Override
      String dbFieldType() {
        return "int4";
      }

      @Override
      String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
      }

      @Override
      String getDefaultDefinition(Column c) {
        IntegerColumn ic = (IntegerColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
          defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
      }
    });

    TYPES_DICT.put(FloatingColumn.class, new ColumnDefiner() {

      @Override
      String dbFieldType() {
        return "float8"; // double precision";
      }

      @Override
      String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
      }

      @Override
      String getDefaultDefinition(Column c) {
        FloatingColumn ic = (FloatingColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
          defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
      }
    });

    TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

      @Override
      String dbFieldType() {
        return "varchar";
      }

      @Override
      String getMainDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String fieldType = ic.isMax() ? "text" : String.format("%s(%s)", dbFieldType(), ic.getLength());
        return join(c.getQuotedName(), fieldType, nullable(c));
      }

      @Override
      String getDefaultDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
          defaultStr = DEFAULT + StringColumn.quoteString(ic.getDefaultValue());
        }
        return defaultStr;
      }
    });

    TYPES_DICT.put(BinaryColumn.class, new ColumnDefiner() {

      @Override
      String dbFieldType() {
        return "bytea";
      }

      @Override
      String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
      }

      @Override
      String getDefaultDefinition(Column c) {
        BinaryColumn bc = (BinaryColumn) c;
        String defaultStr = "";
        if (bc.getDefaultValue() != null) {
          Matcher m = HEXSTR.matcher(bc.getDefaultValue());
          m.matches();
          defaultStr = DEFAULT + String.format("E'\\\\x%s'", m.group(1));
        }
        return defaultStr;
      }
    });

    TYPES_DICT.put(DateTimeColumn.class, new ColumnDefiner() {

      @Override
      String dbFieldType() {
        return "timestamp";
      }

      @Override
      String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
      }

      @Override
      String getDefaultDefinition(Column c) {
        DateTimeColumn ic = (DateTimeColumn) c;
        String defaultStr = "";
        if (ic.isGetdate()) {
          defaultStr = DEFAULT + NOW;
        } else if (ic.getDefaultValue() != null) {
          DateFormat df = new SimpleDateFormat("yyyyMMdd");
          defaultStr = String.format(DEFAULT + " '%s'", df.format(ic.getDefaultValue()));

        }
        return defaultStr;
      }
    });

    TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

      @Override
      String dbFieldType() {
        return "bool";
      }

      @Override
      String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
      }

      @Override
      String getDefaultDefinition(Column c) {
        BooleanColumn ic = (BooleanColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
          defaultStr = DEFAULT + "'" + ic.getDefaultValue() + "'";
        }
        return defaultStr;
      }
    });
  }


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
    // NB: starting from 9.3 version we are able to use
    // 'create schema if not exists' synthax, but we are developing on 9.2
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
  PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask,
                                             List<ParameterSetter> program) throws CelestaException {

    Iterator<String> columns = t.getColumns().keySet().iterator();
    // Создаём параметризуемую часть запроса, пропуская нулевые значения.
    StringBuilder fields = new StringBuilder();
    StringBuilder params = new StringBuilder();
    for (int i = 0; i < t.getColumns().size(); i++) {
      String c = columns.next();
      if (nullsMask[i])
        continue;
      if (params.length() > 0) {
        fields.append(", ");
        params.append(", ");
      }
      params.append("?");
      fields.append('"');
      fields.append(c);
      fields.append('"');
      program.add(ParameterSetter.create(i));
    }

    String returning = "";
    for (Column c : t.getColumns().values())
      if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
        returning = " returning " + c.getQuotedName();
        break;
      }

    String sql = String.format("insert into " + tableTemplate() + " (%s) values (%s)%s;", t.getGrain().getName(),
        t.getName(), fields.toString(), params.toString(), returning);

    // System.out.println(sql);

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
  int getCurrentIdent(Connection conn, Table t) throws CelestaException {
    String sql = String.format("select last_value from \"%s\".\"%s_seq\"", t.getGrain().getName(), t.getName());
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
      } finally {
        stmt.close();
      }
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

  @SuppressWarnings("unchecked")
  @Override
  public DBColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException {
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      ResultSet rs = metaData.getColumns(null, c.getParentTable().getGrain().getName(),
          c.getParentTable().getName(), c.getName());
      try {
        if (rs.next()) {
          DBColumnInfo result = new DBColumnInfo();
          result.setName(rs.getString(COLUMN_NAME));
          String typeName = rs.getString("TYPE_NAME");
          if ("serial".equalsIgnoreCase(typeName)) {
            result.setType(IntegerColumn.class);
            result.setIdentity(true);
            result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
            return result;
          } else if ("text".equalsIgnoreCase(typeName)) {
            result.setType(StringColumn.class);
            result.setMax(true);
          } else {
            for (Class<?> cc : COLUMN_CLASSES)
              if (TYPES_DICT.get(cc).dbFieldType().equalsIgnoreCase(typeName)) {
                result.setType((Class<? extends Column>) cc);
                break;
              }
          }
          result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
          if (result.getType() == StringColumn.class) {
            result.setLength(rs.getInt("COLUMN_SIZE"));
          }
          String defaultBody = rs.getString("COLUMN_DEF");
          if (defaultBody != null) {
            defaultBody = modifyDefault(result, defaultBody);
            result.setDefaultValue(defaultBody);
          }
          return result;
        } else {
          return null;
        }
      } finally {
        rs.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  private String modifyDefault(DBColumnInfo ci, String defaultBody) {
    String result = defaultBody;
    if (DateTimeColumn.class == ci.getType()) {
      if (NOW.equalsIgnoreCase(defaultBody))
        result = "GETDATE()";
      else {
        Matcher m = POSTGRESDATEPATTERN.matcher(defaultBody);
        m.find();
        result = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
      }
    } else if (BooleanColumn.class == ci.getType()) {
      result = "'" + defaultBody.toUpperCase() + "'";
    } else if (StringColumn.class == ci.getType()) {
      if (result.endsWith("::text"))
        result = result.substring(0, result.length() - "::text".length());
      else if (result.endsWith("::character varying"))
        result = result.substring(0, result.length() - "::character varying".length());
    } else if (BinaryColumn.class == ci.getType()) {
      Matcher m = HEX_STRING.matcher(defaultBody);
      if (m.find())
        result = "0x" + m.group(1).toUpperCase();
    }
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

  private void updateColType(Column c, DBColumnInfo actual, List<String> batch) {
    String sql;
    String colType;
    if (c.getClass() == StringColumn.class) {
      StringColumn sc = (StringColumn) c;
      colType = sc.isMax() ? "text" : String.format("%s(%s)", getColumnDefiner(c).dbFieldType(), sc.getLength());
    } else {
      colType = getColumnDefiner(c).dbFieldType();
    }
    // Если тип не совпадает
    if (c.getClass() != actual.getType()) {
      sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" TYPE %s",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName(), colType);
      if (c.getClass() == IntegerColumn.class)
        sql += String.format(" USING (%s::integer);", c.getQuotedName());
      else if (c.getClass() == BooleanColumn.class)
        sql += String.format(" USING (%s::boolean);", c.getQuotedName());

      batch.add(sql);
    } else if (c.getClass() == StringColumn.class) {
      StringColumn sc = (StringColumn) c;
      if (sc.isMax() != actual.isMax() || sc.getLength() != actual.getLength()) {
        sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" TYPE %s",
            c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName(), colType);
        batch.add(sql);
      }
    }
  }

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
  List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {
    // Full foreign key information query
    String sql = String.format(
        "SELECT RC.CONSTRAINT_SCHEMA AS GRAIN" + "   , KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME"
            + "   , KCU1.TABLE_NAME AS FK_TABLE_NAME" + "   , KCU1.COLUMN_NAME AS FK_COLUMN_NAME"
            + "   , KCU2.TABLE_SCHEMA AS REF_GRAIN" + "   , KCU2.TABLE_NAME AS REF_TABLE_NAME"
            + "   , RC.UPDATE_RULE, RC.DELETE_RULE " + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC "
            + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 "
            + "   ON  KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG"
            + "   AND KCU1.CONSTRAINT_SCHEMA  = RC.CONSTRAINT_SCHEMA"
            + "   AND KCU1.CONSTRAINT_NAME    = RC.CONSTRAINT_NAME "
            + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2"
            + "   ON  KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG"
            + "   AND KCU2.CONSTRAINT_SCHEMA  = RC.UNIQUE_CONSTRAINT_SCHEMA"
            + "   AND KCU2.CONSTRAINT_NAME    = RC.UNIQUE_CONSTRAINT_NAME"
            + "   AND KCU2.ORDINAL_POSITION   = KCU1.ORDINAL_POSITION "
            + "WHERE RC.CONSTRAINT_SCHEMA = '%s' " + "ORDER BY KCU1.CONSTRAINT_NAME, KCU1.ORDINAL_POSITION",
        g.getName());

    // System.out.println(sql);

    List<DBFKInfo> result = new LinkedList<>();
    try {
      Statement stmt = conn.createStatement();
      try {
        DBFKInfo i = null;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
          String fkName = rs.getString("FK_CONSTRAINT_NAME");
          if (i == null || !i.getName().equals(fkName)) {
            i = new DBFKInfo(fkName);
            result.add(i);
            i.setTableName(rs.getString("FK_TABLE_NAME"));
            i.setRefGrainName(rs.getString("REF_GRAIN"));
            i.setRefTableName(rs.getString("REF_TABLE_NAME"));
            i.setUpdateRule(getFKRule(rs.getString("UPDATE_RULE")));
            i.setDeleteRule(getFKRule(rs.getString("DELETE_RULE")));
          }
          i.getColumnNames().add(rs.getString("FK_COLUMN_NAME"));
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
    return result;
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
  public void updateVersioningTrigger(Connection conn, Table t) throws CelestaException {
    // First of all, we are about to check if trigger exists
    String sql = String.format("select count(*) from information_schema.triggers where "
        + "		event_object_schema = '%s' and event_object_table= '%s'"
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
                    + " FOR EACH ROW EXECUTE PROCEDURE celesta.recversion_check();",
                t.getGrain().getName(), t.getName());
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
