package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.h2.RecVersionCheckTrigger;
import ru.curs.celesta.dbutils.meta.DBColumnInfo;
import ru.curs.celesta.dbutils.meta.DBFKInfo;
import ru.curs.celesta.dbutils.meta.DBIndexInfo;
import ru.curs.celesta.dbutils.meta.DBPKInfo;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by ioann on 02.05.2017.
 */
final public class H2Adaptor extends SqlDbAdaptor {

  private static final Pattern HEX_STRING = Pattern.compile("X'([0-9A-Fa-f]+)'");

  static {
    TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
      @Override
      String dbFieldType() {
        return "integer";
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
        return "double"; // double precision";
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

    TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

      @Override
      String dbFieldType() {
        return "boolean";
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
        String fieldType = ic.isMax() ? "clob" : String.format("%s(%s)", dbFieldType(), ic.getLength());
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
        return "varbinary";
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
          defaultStr = DEFAULT + String.format("X'%s'", m.group(1));
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
          DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
          defaultStr = String.format(DEFAULT + " '%s'", df.format(ic.getDefaultValue()));

        }
        return defaultStr;
      }
    });
  }


  @Override
  public void configureDb() {
    boolean isH2ReferentialIntegrity = AppSettings.isH2ReferentialIntegrity();

    try {
      //Выполняем команду включения флага REFERENTIAL_INTEGRITY
      Connection connection = ConnectionPool.get();
      String sql = "SET REFERENTIAL_INTEGRITY " + String.valueOf(isH2ReferentialIntegrity);
      Statement stmt = connection.createStatement();

      try {
        stmt.execute(sql);
      } finally {
        stmt.close();
        ConnectionPool.putBack(connection);
      }
    } catch (Exception e) {
      throw new RuntimeException("Can't manage REFERENTIAL_INTEGRITY", e);
    }
  }

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
  public int getCurrentIdent(Connection conn, Table t) throws CelestaException {
    String sql = String.format("select CURRVAL('\"%s\".\"%s_seq\"')", t.getGrain().getName(), t.getName());
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
  public PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask, List<ParameterSetter> program) throws CelestaException {
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


    String sql = String.format("insert into " + tableTemplate() + " (%s) values (%s)", t.getGrain().getName(),
        t.getName(), fields.toString(), params.toString());

    return prepareStatement(conn, sql);
  }

  @Override
  public void manageAutoIncrement(Connection conn, Table t) throws SQLException {
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


  @SuppressWarnings("unchecked")
  @Override
  public DBColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException {
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      String grainName = c.getParentTable().getGrain().getName();
      String tableName = c.getParentTable().getName();
      ResultSet rs = metaData.getColumns(null, grainName, tableName, c.getName());

      try {
        if (rs.next()) {
          DBColumnInfo result = new DBColumnInfo();
          result.setName(rs.getString(COLUMN_NAME));
          String typeName = rs.getString("TYPE_NAME");
          String columnDefault = rs.getString("COLUMN_DEFAULT");


          String columnDefaultForIdentity = String.format(
              "NEXTVAL('" + tableTemplate() + "')", grainName, tableName + "_seq"
          );

          if ("integer".equalsIgnoreCase(typeName) &&
              columnDefaultForIdentity.equals(columnDefault)) {
            result.setType(IntegerColumn.class);
            result.setIdentity(true);
            result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
            return result;
          } else if ("clob".equalsIgnoreCase(typeName)) {
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
            result.setLength(rs.getInt("CHARACTER_MAXIMUM_LENGTH"));
          }

          if (columnDefault != null) {
            columnDefault = modifyDefault(result, columnDefault, conn);
            result.setDefaultValue(columnDefault);
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

  private String modifyDefault(DBColumnInfo ci, String defaultBody, Connection conn) throws CelestaException {
    String result = defaultBody;
    if (DateTimeColumn.class == ci.getType()) {
      if (NOW.equalsIgnoreCase(defaultBody))
        result = "GETDATE()";
      else {
        Matcher m = DATEPATTERN.matcher(defaultBody);
        m.find();
        result = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
      }
    } else if (BooleanColumn.class == ci.getType()) {
      result = "'" + defaultBody.toUpperCase() + "'";
    } else if (BinaryColumn.class == ci.getType()) {
      Matcher m = HEX_STRING.matcher(defaultBody);
      if (m.find())
        result = "0x" + m.group(1).toUpperCase();
    } else if (StringColumn.class == ci.getType()) {
      if (defaultBody.contains("STRINGDECODE")) {
        //H2 отдает default для срок в виде функции, которую нужно выполнить отдельным запросом
        try {
          String sql = "SELECT " + defaultBody;
          Statement stmt = conn.createStatement();

          try {
            ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
              //H2 не сохраняет кавычки в default, если используется не Unicode
              result = "'" + rs.getString(1) + "'";
            } else {
              throw new CelestaException("Can't decode default '" + defaultBody + "'");
            }
          } finally {
            stmt.close();
          }

        } catch (SQLException e) {
          throw new CelestaException("Can't modify default for '" + defaultBody + "'", e);
        }
      }
    }

    return result;
  }


  @Override
  protected void updateColType(Column c, DBColumnInfo actual, List<String> batch) {
    String sql;
    String colType;
    if (c.getClass() == StringColumn.class) {
      StringColumn sc = (StringColumn) c;
      colType = sc.isMax() ? "clob" : String.format("%s(%s)", getColumnDefiner(c).dbFieldType(), sc.getLength());
    } else {
      colType = getColumnDefiner(c).dbFieldType();
    }
    // Если тип не совпадает
    if (c.getClass() != actual.getType()) {
      sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" %s",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName(), colType);

      batch.add(sql);
    } else if (c.getClass() == StringColumn.class) {
      StringColumn sc = (StringColumn) c;
      if (sc.isMax() != actual.isMax() || sc.getLength() != actual.getLength()) {
        sql = String.format(ALTER_TABLE + tableTemplate() + " ALTER COLUMN \"%s\" %s",
            c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName(), colType);
        batch.add(sql);
      }
    }
  }


  @Override
  public DBPKInfo getPKInfo(Connection conn, Table t) throws CelestaException {
    String sql = String.format(
        "SELECT constraint_name AS indexName, column_name as colName " +
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
  public void dropPK(Connection conn, Table t, String pkName) throws CelestaException {
    String sql = String.format("alter table %s.%s drop primary key", t.getGrain().getQuotedName(),
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
  public List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {

    String sql = "SELECT " +
        "FK_NAME AS FK_CONSTRAINT_NAME, " +
        "FKTABLE_NAME AS FK_TABLE_NAME, " +
        "FKCOLUMN_NAME AS FK_COLUMN_NAME, " +
        "PKTABLE_SCHEMA AS REF_GRAIN, " +
        "PKTABLE_NAME AS REF_TABLE_NAME, " +
        "UPDATE_RULE, " +
        "DELETE_RULE " +
        "FROM INFORMATION_SCHEMA.CROSS_REFERENCES " +
        "WHERE FKTABLE_SCHEMA = '%s' " +
        "ORDER BY FK_CONSTRAINT_NAME, ORDINAL_POSITION";
    sql = String.format(sql, g.getName());

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

            String updateRule = resolveConstraintReferential(rs.getInt("UPDATE_RULE"));
            i.setUpdateRule(getFKRule(updateRule));
            String deleteRule = resolveConstraintReferential(rs.getInt("DELETE_RULE"));
            i.setDeleteRule(getFKRule(deleteRule));
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

  private String resolveConstraintReferential(int constraintReferential) {
    final String result;

    switch (constraintReferential) {
      case DatabaseMetaData.importedKeyCascade:
        result = "CASCADE";
        break;
      case DatabaseMetaData.importedKeyRestrict:
        result = "RESTRICT";
        break;
      case DatabaseMetaData.importedKeySetNull:
        result = "SET NULL";
        break;
      case DatabaseMetaData.importedKeyNoAction:
        result = "NO ACTION";
        break;
      default:
        result = "";
    }

    return result;
  }

  @Override
  String[] getCreateIndexSQL(Index index) {
    String grainName = index.getTable().getGrain().getName();
    String fieldList = getFieldList(index.getColumns().keySet());
    String sql = String.format("CREATE INDEX " + tableTemplate() + " ON " + tableTemplate() + " (%s)", grainName,
        index.getName(), grainName, index.getTable().getName(), fieldList);
    String[] result = { sql };
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
      sql = getSelectFromOrderBy(t, whereClause, orderBy) + String.format(" limit -1 offset %d", offset);
    else {
      sql = getSelectFromOrderBy(t, whereClause, orderBy)
          + String.format(" limit %d offset %d", rowCount, offset);
    }
    return sql;
  }

  @Override
  public Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException {
    Map<String, DBIndexInfo> result = new HashMap<>();

    String sql = String.format(
        "SELECT table_name as tableName, index_name as indexName, column_name as colName " +
            "FROM  INFORMATION_SCHEMA.INDEXES " +
            "WHERE table_schema = '%s' AND primary_key <> true",
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
        + "		and trigger_name = 'versioncheck_%s'", t.getGrain().getName(), t.getName(), t.getName());
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
                "CREATE TRIGGER \"versioncheck_%s\"" + " BEFORE UPDATE ON " + tableTemplate()
                    + " FOR EACH ROW CALL \"%s\"",
                t.getName(), t.getGrain().getName(), t.getName(),
                RecVersionCheckTrigger.class.getName());

            stmt.executeUpdate(sql);
          }
        } else {
          if (triggerExists) {
            // DROP TRIGGER
            sql = String.format("DROP TRIGGER \"versioncheck_%s\" ON " + tableTemplate(),
                t.getGrain().getName(), t.getName(), t.getName());
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


  @Override
  public String translateDate(String date) throws CelestaException {
    try {
      Date d = DateTimeColumn.parseISODate(date);
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      return String.format("date '%s'", df.format(d));
    } catch (ParseException e) {
      throw new CelestaException(e.getMessage());
    }

  }
}