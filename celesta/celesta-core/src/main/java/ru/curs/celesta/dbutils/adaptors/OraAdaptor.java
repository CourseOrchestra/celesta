/*
   (с) 2013 ООО "КУРС-ИТ"  

   Этот файл — часть КУРС:Celesta.
   
   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.

   
   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */

package ru.curs.celesta.dbutils.adaptors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.meta.DBColumnInfo;
import ru.curs.celesta.dbutils.meta.DBFKInfo;
import ru.curs.celesta.dbutils.meta.DBIndexInfo;
import ru.curs.celesta.dbutils.meta.DBPKInfo;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

/**
 * Адаптер Oracle Database.
 */
final class OraAdaptor extends DBAdaptor {
  private static final String SELECT_S_FROM = "select %s from ";

  private static final String SELECT_TRIGGER_BODY = "select TRIGGER_BODY  from all_triggers "
      + "where owner = sys_context('userenv','session_user') ";

  private static final String CSC = "csc_";

  private static final String SNL = "snl_";

  private static final String DROP_TRIGGER = "drop trigger \"";

  private static final String TABLE_TEMPLATE = "\"%s_%s\"";

  private static final Pattern BOOLEAN_CHECK = Pattern.compile("\"([^\"]+)\" *[iI][nN] *\\( *0 *, *1 *\\)");
  private static final Pattern DATE_PATTERN = Pattern.compile("'(\\d\\d\\d\\d)-([01]\\d)-([0123]\\d)'");
  private static final Pattern HEX_STRING = Pattern.compile("'([0-9A-F]+)'");
  private static final Pattern TABLE_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");

  private static final Map<Class<? extends Column>, OraColumnDefiner> TYPES_DICT = new HashMap<>();
  private static final Map<TriggerType, String> TRIGGER_EVENT_TYPE_DICT = new HashMap<>();

  /**
   * Определитель колонок для Oracle, учитывающий тот факт, что в Oracle
   * DEFAULT должен идти до NOT NULL.
   */
  abstract static class OraColumnDefiner extends ColumnDefiner {
    abstract String getInternalDefinition(Column c);

    @Override
    String getFullDefinition(Column c) {
      return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c));
    }

    @Override
    final String getMainDefinition(Column c) {
      return join(getInternalDefinition(c), nullable(c));
    }
  }

  static {
    TYPES_DICT.put(IntegerColumn.class, new OraColumnDefiner() {
          @Override
          String dbFieldType() {
            return "number";
          }

          @Override
          String getInternalDefinition(Column c) {
            return join(c.getQuotedName(), dbFieldType());
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
        }

    );

    TYPES_DICT.put(FloatingColumn.class, new OraColumnDefiner() {

          @Override
          String dbFieldType() {
            return "real";
          }

          @Override
          String getInternalDefinition(Column c) {
            return join(c.getQuotedName(), dbFieldType());
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

        }

    );
    TYPES_DICT.put(StringColumn.class, new OraColumnDefiner() {

      @Override
      String dbFieldType() {
        return "nvarchar2";
      }

      // Пустая DEFAULT-строка не сочетается с NOT NULL в Oracle.
      @Override
      String nullable(Column c) {
        StringColumn ic = (StringColumn) c;
        return ("".equals(ic.getDefaultValue())) ? "null" : super.nullable(c);
      }

      @Override
      String getInternalDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String fieldType = ic.isMax() ? "nclob" : String.format("%s(%s)", dbFieldType(), ic.getLength());
        return join(c.getQuotedName(), fieldType);
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
    TYPES_DICT.put(BinaryColumn.class, new OraColumnDefiner() {
      @Override
      String dbFieldType() {
        return "blob";
      }

      @Override
      String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
      }

      @Override
      String getDefaultDefinition(Column c) {
        BinaryColumn ic = (BinaryColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
          // Отрезаем 0x и закавычиваем
          defaultStr = String.format(DEFAULT + "'%s'", ic.getDefaultValue().substring(2));
        }
        return defaultStr;
      }
    });

    TYPES_DICT.put(DateTimeColumn.class, new OraColumnDefiner() {

      @Override
      String dbFieldType() {
        return "timestamp";
      }

      @Override
      String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
      }

      @Override
      String getDefaultDefinition(Column c) {
        DateTimeColumn ic = (DateTimeColumn) c;
        String defaultStr = "";
        if (ic.isGetdate()) {
          defaultStr = DEFAULT + "sysdate";
        } else if (ic.getDefaultValue() != null) {
          DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
          defaultStr = String.format(DEFAULT + "date '%s'", df.format(ic.getDefaultValue()));
        }
        return defaultStr;
      }
    });
    TYPES_DICT.put(BooleanColumn.class, new OraColumnDefiner() {

      @Override
      String dbFieldType() {
        return "number";
      }

      @Override
      String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
      }

      @Override
      String getDefaultDefinition(Column c) {
        BooleanColumn ic = (BooleanColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
          defaultStr = DEFAULT + (ic.getDefaultValue() ? "1" : "0");
        }
        return defaultStr;
      }

      @Override
      String getFullDefinition(Column c) {
        String check = String.format("constraint %s check (%s in (0, 1))", getBooleanCheckName(c),
            c.getQuotedName());
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c), check);
      }

    });
  }

  static {
    //В Oracle есть также BEFORE и AFTER триггеры.
    // Но EVEN_TYPE может принимать только 3 значения: INSERT/UPDATE/DELETE
    TRIGGER_EVENT_TYPE_DICT.put(TriggerType.PRE_INSERT, "INSERT");
    TRIGGER_EVENT_TYPE_DICT.put(TriggerType.PRE_UPDATE, "UPDATE");
    TRIGGER_EVENT_TYPE_DICT.put(TriggerType.PRE_DELETE, "DELETE");
    TRIGGER_EVENT_TYPE_DICT.put(TriggerType.POST_INSERT, "INSERT");
    TRIGGER_EVENT_TYPE_DICT.put(TriggerType.POST_UPDATE, "UPDATE");
    TRIGGER_EVENT_TYPE_DICT.put(TriggerType.POST_DELETE, "DELETE");
  }

  public OraAdaptor(ConnectionPool connectionPool) {
    super(connectionPool);
  }

  @Override
  public boolean tableExists(Connection conn, String schema, String name) throws CelestaException {
    if (schema == null || schema.isEmpty() || name == null || name.isEmpty()) {
      return false;
    }
    String sql = String.format("select count(*) from all_tables where owner = "
        + "sys_context('userenv','session_user') and table_name = '%s_%s'", schema, name);

    try (Statement checkForTable = conn.createStatement()) {
      ResultSet rs = checkForTable.executeQuery(sql);
      return rs.next() && rs.getInt(1) > 0;
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }


  @Override
  boolean userTablesExist(Connection conn) throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM USER_TABLES");
    ResultSet rs = pstmt.executeQuery();
    try {
      rs.next();
      return rs.getInt(1) > 0;
    } finally {
      rs.close();
      pstmt.close();
    }
  }

  @Override
  void createSchemaIfNotExists(Connection conn, String schema) throws SQLException {
    // Ничего не делает для Oracle. Схемы имитируются префиксами на именах
    // таблиц.
  }

  @Override
  OraColumnDefiner getColumnDefiner(Column c) {
    return TYPES_DICT.get(c.getClass());
  }

  @Override
  public PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) throws CelestaException {
    TableElement t = c.getParentTable();
    String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s and rownum = 1", c.getQuotedName(),
        t.getGrain().getName(), t.getName(), where);
    return prepareStatement(conn, sql);
  }

  @Override
  public PreparedStatement getOneRecordStatement(
      Connection conn, TableElement t, String where, Set<String> fields
  ) throws CelestaException {

    final String fieldList = getTableFieldsListExceptBlobs((GrainElement) t, fields);

    String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s and rownum = 1",
        fieldList, t.getGrain().getName(), t.getName(), where);
    return prepareStatement(conn, sql);
  }

  @Override
  public PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask,
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

    final String sql;

    if (fields.length() == 0 && params.length() == 0) {
      //Для выполнения пустого insert ищем любое поле, отличное от recversion
      String columnToInsert = t.getColumns().keySet()
          .stream()
          .filter(k -> !VersionedElement.REC_VERSION.equals(k))
          .findFirst().get();

      sql = String.format("insert into " + tableTemplate() + " (\"%s\") values (DEFAULT)", t.getGrain().getName(),
          t.getName(), columnToInsert);
    } else {
      sql = String.format("insert into " + tableTemplate() + " (%s) values (%s)", t.getGrain().getName(),
          t.getName(), fields.toString(), params.toString());
    }
    return prepareStatement(conn, sql);
  }

  @Override
  public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) throws CelestaException {
    String sql = String.format("delete " + tableTemplate() + " where %s", t.getGrain().getName(), t.getName(),
        where);
    return prepareStatement(conn, sql);
  }

  @Override
  public Set<String> getColumns(Connection conn, TableElement t) throws CelestaException {
    Set<String> result = new LinkedHashSet<>();
    try {
      String tableName = String.format("%s_%s", t.getGrain().getName(), t.getName());
      String sql = String.format(
          "SELECT column_name FROM user_tab_cols WHERE table_name = '%s' order by column_id", tableName);
      // System.out.println(sql);
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      try {
        while (rs.next()) {
          String rColumnName = rs.getString(COLUMN_NAME);
          result.add(rColumnName);
        }
      } finally {
        rs.close();
      }
      stmt.close();
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
    return result;
  }

  @Override
  public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) throws CelestaException {
    String sql = String.format("delete from " + tableTemplate() + " %s", t.getGrain().getName(), t.getName(),
        where.isEmpty() ? "" : "where " + where);
    try {
      PreparedStatement result = conn.prepareStatement(sql);
      return result;
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  @Override
  public boolean isValidConnection(Connection conn, int timeout) throws CelestaException {
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT 1 FROM Dual");
      return rs.next();
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public String tableTemplate() {
    return TABLE_TEMPLATE;
  }

  @Override
  public int getCurrentIdent(Connection conn, Table t) throws CelestaException {
    String sequenceName = getSequenceName(t);
    String sql = String.format("SELECT \"%s\".CURRVAL FROM DUAL", sequenceName);
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
  public String getInFilterClause(Table table, Table otherTable, List<String> fields,
                                  List<String> otherFields, String otherWhere) {
    String template = "( %s ) IN (SELECT %s FROM %s WHERE %s)";
    String fieldsStr = String.join(",",
        fields.stream()
            .map(s -> "\"" + s + "\"")
            .collect(Collectors.toList())
    );
    String otherFieldsStr = String.join(",",
        otherFields.stream()
            .map(s -> "\"" + s + "\"")
            .collect(Collectors.toList())
    );

    String otherTableStr = String.format(tableTemplate(), otherTable.getGrain().getName(), otherTable.getName());
    String result = String.format(template, fieldsStr, otherFieldsStr, otherTableStr, otherWhere);
    return result;
  }

  @Override
  String[] getCreateIndexSQL(Index index) {
    String grainName = index.getTable().getGrain().getName();
    String fieldList = getFieldList(index.getColumns().keySet());
    String sql = String.format("CREATE INDEX " + tableTemplate() + " ON " + tableTemplate() + " (%s)", grainName,
        index.getName(), grainName, index.getTable().getName(), fieldList);
    String[] result = {sql};
    return result;
  }

  @Override
  String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
    String sql;
    if (dBIndexInfo.getIndexName().startsWith("##")) {
      sql = String.format("DROP INDEX %s", dBIndexInfo.getIndexName().substring(2));
    } else {
      sql = String.format("DROP INDEX " + tableTemplate(), g.getName(), dBIndexInfo.getIndexName());
    }
    String[] result = {sql};
    return result;
  }

  private boolean checkForBoolean(Connection conn, Column c) throws SQLException {
    String sql = String.format(
        "SELECT SEARCH_CONDITION FROM ALL_CONSTRAINTS WHERE " + "OWNER = sys_context('userenv','session_user')"
            + " AND TABLE_NAME = '%s_%s'" + "AND CONSTRAINT_TYPE = 'C'",
        c.getParentTable().getGrain().getName(), c.getParentTable().getName());
    // System.out.println(sql);
    PreparedStatement checkForBool = conn.prepareStatement(sql);
    try {
      ResultSet rs = checkForBool.executeQuery();
      while (rs.next()) {
        String buf = rs.getString(1);
        Matcher m = BOOLEAN_CHECK.matcher(buf);
        if (m.find() && m.group(1).equals(c.getName()))
          return true;
      }
    } finally {
      checkForBool.close();
    }
    return false;

  }

  private boolean checkForIncrementTrigger(Connection conn, Column c) throws SQLException {
    String sql = String.format(
        SELECT_TRIGGER_BODY
            + "and table_name = '%s_%s' and trigger_name = '%s' and triggering_event = 'INSERT'",
        c.getParentTable().getGrain().getName(), c.getParentTable().getName(),
        getSequenceName(c.getParentTable()));
    // System.out.println(sql);
    PreparedStatement checkForTrigger = conn.prepareStatement(sql);
    try {
      ResultSet rs = checkForTrigger.executeQuery();
      if (rs.next()) {
        String body = rs.getString(1);
        if (body != null && body.contains(".NEXTVAL") && body.contains("\"" + c.getName() + "\""))
          return true;
      }
    } finally {
      checkForTrigger.close();
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DBColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException {
    try {
      String tableName = String.format("%s_%s", c.getParentTable().getGrain().getName(),
          c.getParentTable().getName());
      String sql = String.format(
          "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, CHAR_LENGTH "
              + "FROM user_tab_cols	WHERE table_name = '%s' and COLUMN_NAME = '%s'",
          tableName, c.getName());
      // System.out.println(sql);
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      DBColumnInfo result;
      try {
        if (rs.next()) {
          result = new DBColumnInfo();
          result.setName(rs.getString(COLUMN_NAME));
          String typeName = rs.getString("DATA_TYPE");

          if (typeName.startsWith("TIMESTAMP")) {
            result.setType(DateTimeColumn.class);
          } else if ("float".equalsIgnoreCase(typeName)) {
            result.setType(FloatingColumn.class);
          } else if ("nclob".equalsIgnoreCase(typeName)) {
            result.setType(StringColumn.class);
            result.setMax(true);
          } else {
            for (Class<?> cc : COLUMN_CLASSES)
              if (TYPES_DICT.get(cc).dbFieldType().equalsIgnoreCase(typeName)) {
                result.setType((Class<? extends Column>) cc);
                break;
              }
          }
          if (IntegerColumn.class == result.getType()) {
            // В Oracle булевские столбцы имеют тот же тип данных,
            // что и INT-столбцы: просматриваем, есть ли на них
            // ограничение CHECK.
            if (checkForBoolean(conn, c))
              result.setType(BooleanColumn.class);
              // В Oracle признак IDENTITY имитируется триггером.
              // Просматриваем, есть ли на поле триггер, обладающий
              // признаками того, что это -- созданный Celesta
              // системный триггер.
            else if (checkForIncrementTrigger(conn, c))
              result.setIdentity(true);
          }
          result.setNullable("Y".equalsIgnoreCase(rs.getString("NULLABLE")));
          if (result.getType() == StringColumn.class) {
            result.setLength(rs.getInt("CHAR_LENGTH"));
          }

        } else {
          return null;
        }
      } finally {
        rs.close();
        stmt.close();
      }
      // Извлекаем значение DEFAULT отдельно.
      processDefaults(conn, c, result);

      return result;
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }

  }

  private void processDefaults(Connection conn, Column c, DBColumnInfo result) throws SQLException {
    ResultSet rs;
    PreparedStatement getDefault = conn.prepareStatement(String.format(
        "select DATA_DEFAULT from DBA_TAB_COLUMNS where " + "owner = sys_context('userenv','session_user') "
            + "and TABLE_NAME = '%s_%s' and COLUMN_NAME = '%s'",
        c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName()));
    try {
      rs = getDefault.executeQuery();
      if (!rs.next())
        return;
      String body = rs.getString(1);
      if (body == null || "null".equalsIgnoreCase(body))
        return;
      if (BooleanColumn.class == result.getType())
        body = "0".equals(body.trim()) ? "'FALSE'" : "'TRUE'";
      else if (DateTimeColumn.class == result.getType()) {
        if (body.toLowerCase().contains("sysdate"))
          body = "GETDATE()";
        else {
          Matcher m = DATE_PATTERN.matcher(body);
          if (m.find())
            body = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
        }
      } else if (BinaryColumn.class == result.getType()) {
        Matcher m = HEX_STRING.matcher(body);
        if (m.find())
          body = "0x" + m.group(1);
      } else {
        body = body.trim();
      }
      result.setDefaultValue(body);

    } finally {
      getDefault.close();
    }
  }

  private boolean isNclob(Column c) {
    return c instanceof StringColumn && ((StringColumn) c).isMax();
  }

  @Override
  public void updateColumn(Connection conn, Column c, DBColumnInfo actual) throws CelestaException {
    dropVersioningTrigger(conn, c.getParentTable());
    if (actual.getType() == BooleanColumn.class && !(c instanceof BooleanColumn)) {
      // Тип Boolean меняется на что-то другое, надо сбросить constraint
      String check = String.format(ALTER_TABLE + tableTemplate() + " drop constraint %s",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), getBooleanCheckName(c));
      runUpdateColumnSQL(conn, c, check);
    }

    OraColumnDefiner definer = getColumnDefiner(c);

    // В Oracle нельзя снять default, можно только установить его в Null
    String defdef = definer.getDefaultDefinition(c);
    if ("".equals(defdef) && !"".equals(actual.getDefaultValue()))
      defdef = "default null";

    // В Oracle, если меняешь blob-поле, то в alter table не надо
    // указывать его тип (будет ошибка).
    String def;
    if (actual.getType() == BinaryColumn.class && c instanceof BinaryColumn) {
      def = OraColumnDefiner.join(c.getQuotedName(), defdef);
    } else {
      def = OraColumnDefiner.join(definer.getInternalDefinition(c), defdef);
    }

    // Явно задавать nullable в Oracle можно только если действительно надо
    // изменить
    if (actual.isNullable() != c.isNullable())
      def = OraColumnDefiner.join(def, definer.nullable(c));

    // Перенос из NCLOB и в NCLOB надо производить с осторожностью

    if (fromOrToNClob(c, actual)) {

      String tempName = "\"" + c.getName() + "2\"";
      String sql = String.format(ALTER_TABLE + tableTemplate() + " add %s",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), columnDef(c));
      sql = sql.replace(c.getQuotedName(), tempName);
      // System.out.println(sql);
      runUpdateColumnSQL(conn, c, sql);
      sql = String.format("update " + tableTemplate() + " set %s = \"%s\"",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), tempName, c.getName());
      // System.out.println(sql);
      runUpdateColumnSQL(conn, c, sql);
      sql = String.format(ALTER_TABLE + tableTemplate() + " drop column %s",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getQuotedName());
      // System.out.println(sql);
      runUpdateColumnSQL(conn, c, sql);
      sql = String.format(ALTER_TABLE + tableTemplate() + " rename column %s to %s",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), tempName, c.getQuotedName());
      // System.out.println(sql);
      runUpdateColumnSQL(conn, c, sql);
    } else {

      String sql = String.format(ALTER_TABLE + tableTemplate() + " modify (%s)",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), def);

      runUpdateColumnSQL(conn, c, sql);
    }
    if (c instanceof BooleanColumn && actual.getType() != BooleanColumn.class) {
      // Тип поменялся на Boolean, надо добавить constraint
      String check = String.format(ALTER_TABLE + tableTemplate() + " add constraint %s check (%s in (0, 1))",
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), getBooleanCheckName(c),
          c.getQuotedName());
      runUpdateColumnSQL(conn, c, check);

    }
  }

  public boolean fromOrToNClob(Column c, DBColumnInfo actual) {
    return (actual.isMax() || isNclob(c)) && !(actual.isMax() && isNclob(c));
  }

  private static String getFKTriggerName(String prefix, String fkName) {
    String result = prefix + fkName;
    result = NamedElement.limitName(result);
    return result;
  }

  private static String getBooleanCheckName(Column c) {
    String result = String.format("chk_%s_%s_%s", c.getParentTable().getGrain().getName(),
        c.getParentTable().getName(), c.getName());
    result = NamedElement.limitName(result);
    return "\"" + result + "\"";
  }

  private static String getSequenceName(TableElement table) {
    String result = String.format("%s_%s_inc", table.getGrain().getName(), table.getName());
    result = NamedElement.limitName(result);
    return result;
  }

  @Override
  public void manageAutoIncrement(Connection conn, TableElement t) throws SQLException {
    // 1. Firstly, we have to clean up table from any auto-increment
    // triggers
    String sequenceName = getSequenceName(t);
    TriggerQuery dropQuery = new TriggerQuery().withName(sequenceName);
    try {
      dropTrigger(conn, dropQuery);
    } catch (SQLException e) {
      // do nothing
    }

    // 2. Check if table has IDENTITY field, if it doesn't, no need to
    // proceed.
    IntegerColumn ic = findIdentityField(t);
    if (ic == null)
      return;

    String sql;
    PreparedStatement stmt;
    // 2. Now, we know that we surely have IDENTITY field, and we have to
    // be sure that we have an appropriate sequence.
    boolean hasSequence = false;
    stmt = conn
        .prepareStatement(
            String.format(
                "select count(*) from all_sequences where sequence_owner = "
                    + "sys_context('userenv','session_user') and sequence_name = '%s'",
                sequenceName));
    ResultSet rs = stmt.executeQuery();
    try {
      hasSequence = rs.next() && rs.getInt(1) > 0;
    } finally {
      stmt.close();
    }
    if (!hasSequence) {
      sql = String.format("CREATE SEQUENCE \"%s\"" + " START WITH 1 INCREMENT BY 1 MINVALUE 1 NOCACHE NOCYCLE",
          sequenceName);
      stmt = conn.prepareStatement(sql);
      try {
        stmt.executeUpdate();
      } finally {
        stmt.close();
      }
    }

    // 3. Now we have to create or replace the auto-increment trigger
    sql = String.format(
        "CREATE OR REPLACE TRIGGER \"" + sequenceName + "\" BEFORE INSERT ON " + tableTemplate()
            + " FOR EACH ROW WHEN (new.%s is null) BEGIN SELECT \"" + sequenceName
            + "\".NEXTVAL INTO :new.%s FROM dual; END;",
        t.getGrain().getName(), t.getName(), ic.getQuotedName(), ic.getQuotedName());

    // System.out.println(sql);
    Statement s = conn.createStatement();
    try {
      s.execute(sql);
    } finally {
      stmt.close();
    }
  }

  @Override
  void dropAutoIncrement(Connection conn, TableElement t) throws SQLException {
    // Удаление Sequence
    String sequenceName = getSequenceName(t);
    String sql = "DROP SEQUENCE \"" + sequenceName + "\"";
    Statement stmt = conn.createStatement();
    try {
      stmt.execute(sql);
    } catch (SQLException e) {
      // do nothing
      sql = "";
    } finally {

      stmt.close();
    }

  }

  @Override
  public DBPKInfo getPKInfo(Connection conn, TableElement t) throws CelestaException {
    DBPKInfo result = new DBPKInfo();
    try {
      String sql = String.format("select cons.constraint_name, column_name from all_constraints cons "
              + "inner join all_cons_columns cols on cons.constraint_name = cols.constraint_name  "
              + "and cons.owner = cols.owner where " + "cons.owner = sys_context('userenv','session_user') "
              + "and cons.table_name = '%s_%s'" + " and cons.constraint_type = 'P' order by cols.position",
          t.getGrain().getName(), t.getName());
      Statement check = conn.createStatement();
      ResultSet rs = check.executeQuery(sql);
      try {
        while (rs.next()) {
          result.setName(rs.getString(1));
          result.getColumnNames().add(rs.getString(2));
        }
      } finally {
        rs.close();
        check.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }

    return result;

  }

  @Override
  public void dropPK(Connection conn, TableElement t, String pkName) throws CelestaException {
    String sql = String.format("alter table \"%s_%s\" drop constraint \"%s\"", t.getGrain().getName(), t.getName(),
        pkName);
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }

  }

  @Override
  public void createPK(Connection conn, TableElement t) throws CelestaException {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("alter table \"%s_%s\" add constraint \"%s\" " + " primary key (",
        t.getGrain().getName(), t.getName(), t.getPkConstraintName()));
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

    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql.toString());
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }

  }

  @Override
  public List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {
    String sql = String.format(
        "select cols.constraint_name, cols.table_name table_name, "
            + "ref.table_name ref_table_name, cons.delete_rule, cols.column_name "
            + "from all_constraints cons inner join all_cons_columns cols "
            + "on cols.owner = cons.owner and cols.constraint_name = cons.constraint_name "
            + "  and cols.table_name = cons.table_name "
            + "inner join all_constraints ref on ref.owner = cons.owner "
            + "  and ref.constraint_name = cons.r_constraint_name " + "where cons.constraint_type = 'R' "
            + "and cons.owner = sys_context('userenv','session_user') " + "and ref.constraint_type = 'P' "
            + "and  cons.table_name like '%s@_%%' escape '@' order by cols.constraint_name, cols.position",
        g.getName());

    // System.out.println(sql);
    List<DBFKInfo> result = new LinkedList<>();
    try {
      Statement stmt = conn.createStatement();
      try {
        DBFKInfo i = null;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
          String fkName = rs.getString("CONSTRAINT_NAME");
          if (i == null || !i.getName().equals(fkName)) {
            i = new DBFKInfo(fkName);
            result.add(i);
            String tableName = rs.getString("TABLE_NAME");
            Matcher m = TABLE_PATTERN.matcher(tableName);
            m.find();
            i.setTableName(m.group(2));
            tableName = rs.getString("REF_TABLE_NAME");
            m = TABLE_PATTERN.matcher(tableName);
            m.find();
            i.setRefGrainName(m.group(1));
            i.setRefTableName(m.group(2));
            i.setUpdateRule(getUpdateBehaviour(conn, tableName, fkName));
            i.setDeleteRule(getFKRule(rs.getString("DELETE_RULE")));
          }
          i.getColumnNames().add(rs.getString(COLUMN_NAME));
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
    return result;

  }

  private FKRule getUpdateBehaviour(Connection conn, String tableName, String fkName) throws SQLException {
    // now we are looking for triggers that define update
    // rule
    String sql = String.format(
        "select trigger_name from all_triggers " + "where owner = sys_context('userenv','session_user') "
            + "and table_name = '%s' and trigger_name in ('%s', '%s') and triggering_event = 'UPDATE'",
        tableName, getFKTriggerName(SNL, fkName), getFKTriggerName(CSC, fkName));
    Statement stmt = conn.createStatement();
    try {
      ResultSet rs = stmt.executeQuery(sql);
      if (rs.next()) {
        sql = rs.getString("TRIGGER_NAME");
        if (sql.startsWith(CSC))
          return FKRule.CASCADE;
        else if (sql.startsWith(SNL))
          return FKRule.SET_NULL;
      }
      return FKRule.NO_ACTION;
    } finally {
      stmt.close();
    }
  }

  @Override
  void processCreateUpdateRule(ForeignKey fk, LinkedList<StringBuilder> sql) {
    StringBuilder sb;

    // Clean up unwanted triggers
    switch (fk.getUpdateRule()) {
      case CASCADE:
        sb = new StringBuilder(DROP_TRIGGER);
        sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
        sb.append("\"");
        sql.add(sb);
        break;
      case SET_NULL:
        sb = new StringBuilder(DROP_TRIGGER);
        sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
        sb.append("\"");
        sql.add(sb);
        break;
      case NO_ACTION:
      default:
        sb = new StringBuilder(DROP_TRIGGER);
        sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
        sb.append("\"");
        sql.add(sb);
        sb = new StringBuilder(DROP_TRIGGER);
        sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
        sb.append("\"");
        sql.add(sb);
        return;
    }

    sb = new StringBuilder();
    sb.append("create or replace trigger \"");
    if (fk.getUpdateRule() == FKRule.CASCADE) {
      sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
    } else {
      sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
    }
    sb.append("\" after update of ");
    Table t = fk.getReferencedTable();
    boolean needComma = false;
    for (Column c : t.getPrimaryKey().values()) {
      if (needComma)
        sb.append(", ");
      sb.append(c.getQuotedName());
      needComma = true;
    }
    sb.append(String.format(" on \"%s_%s\"", t.getGrain().getName(), t.getName()));
    sb.append(String.format(" for each row begin\n  update \"%s_%s\" set ",
        fk.getParentTable().getGrain().getName(), fk.getParentTable().getName()));

    Iterator<Column> i1 = fk.getColumns().values().iterator();
    Iterator<Column> i2 = t.getPrimaryKey().values().iterator();
    needComma = false;
    while (i1.hasNext()) {
      sb.append(needComma ? ",\n    " : "\n    ");
      needComma = true;
      sb.append(i1.next().getQuotedName());
      sb.append(" = :new.");
      sb.append(i2.next().getQuotedName());
    }
    sb.append("\n  where ");
    i1 = fk.getColumns().values().iterator();
    i2 = t.getPrimaryKey().values().iterator();
    needComma = false;
    while (i1.hasNext()) {
      sb.append(needComma ? ",\n    " : "\n    ");
      needComma = true;
      sb.append(i1.next().getQuotedName());
      if (fk.getUpdateRule() == FKRule.CASCADE) {
        sb.append(" = :old.");
        sb.append(i2.next().getQuotedName());
      } else {
        sb.append(" = null");
      }
    }
    sb.append(";\nend;");
    sql.add(sb);
  }

  @Override
  void processDropUpdateRule(LinkedList<String> sqlQueue, String fkName) {
    String sql = String.format(DROP_TRIGGER + "%s\"", getFKTriggerName(SNL, fkName));
    sqlQueue.add(sql);
    sql = String.format(DROP_TRIGGER + "%s\"", getFKTriggerName(CSC, fkName));
    sqlQueue.add(sql);
  }

  @Override
  String getLimitedSQL(
      FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields
  ) {
    if (offset == 0 && rowCount == 0)
      throw new IllegalArgumentException();
    String sql;
    if (offset == 0) {
      // No offset -- simpler query
      sql = String.format("with a as (%s) select a.* from a where rownum <= %d",
          getSelectFromOrderBy(from, whereClause, orderBy, fields), rowCount);
    } else if (rowCount == 0) {
      // No rowCount -- simpler query
      sql = String.format(
          "with a as (%s) select * from (select a.*, ROWNUM rnum " + "from a) where rnum >= %d order by rnum",
          getSelectFromOrderBy(from, whereClause, orderBy, fields), offset + 1L);

    } else {
      sql = getLimitedSqlWithOffset(orderBy, fields, from, whereClause, offset, rowCount);
    }
    return sql;
  }

  private String getLimitedSqlWithOffset(String orderBy, Set<String> fields, FromClause from, String where, long offset, long rowCount) {
    return String.format(
            "with a as (%s) select * from (select a.*, ROWNUM rnum "
                    + "from a where rownum <= %d) where rnum >= %d order by rnum",
            getSelectFromOrderBy(from, where, orderBy, fields), offset + rowCount, offset + 1L);
  }

  @Override
  public Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException {
    String sql = String
        .format("select ind.table_name TABLE_NAME, ind.index_name INDEX_NAME, cols.column_name COLUMN_NAME,"
            + " cols.column_position POSITION " + "from all_indexes ind "
            + "inner join all_ind_columns cols " + "on ind.owner = cols.index_owner "
            + "and ind.table_name = cols.table_name " + "and ind.index_name = cols.index_name "
            + "where ind.owner = sys_context('userenv','session_user') and ind.uniqueness = 'NONUNIQUE' "
            + "and ind.table_name like '%s@_%%' escape '@'"
            + "order by ind.table_name, ind.index_name, cols.column_position", g.getName());

    // System.out.println(sql);

    Map<String, DBIndexInfo> result = new HashMap<>();
    try {
      Statement stmt = conn.createStatement();
      try {
        DBIndexInfo i = null;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
          String tabName = rs.getString("TABLE_NAME");
          Matcher m = TABLE_PATTERN.matcher(tabName);
          m.find();
          tabName = m.group(2);
          String indName = rs.getString("INDEX_NAME");
          m = TABLE_PATTERN.matcher(indName);
          if (m.find()) {
            indName = m.group(2);
          } else {
            /*
             * Если название индекса не соответствует ожидаемому
						 * шаблону, то это -- индекс, добавленный вне Celesta и
						 * его следует удалить. Мы добавляем знаки ## перед
						 * именем индекса. Далее система, не найдя индекс с
						 * такими метаданными, поставит такой индекс на
						 * удаление. Метод удаления, обнаружив ## в начале имени
						 * индекса, удалит их.
						 */
            indName = "##" + indName;
          }

          if (i == null || !i.getTableName().equals(tabName) || !i.getIndexName().equals(indName)) {
            i = new DBIndexInfo(tabName, indName);
            result.put(indName, i);
          }
          i.getColumnNames().add(rs.getString("COLUMN_NAME"));
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
  public List<String> getViewList(Connection conn, Grain g) throws CelestaException {
    String sql = String.format(
        "select view_name from all_views "
            + "where owner = sys_context('userenv','session_user') and view_name like '%s@_%%' escape '@'",
        g.getName());
    List<String> result = new LinkedList<>();
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        String buf = rs.getString(1);
        Matcher m = TABLE_PATTERN.matcher(buf);
        m.find();
        result.add(m.group(2));
      }
    } catch (SQLException e) {
      throw new CelestaException("Cannot get views list: %s", e.toString());
    }
    return result;
  }

  @Override
  public List<String> getParameterizedViewList(Connection conn, Grain g) throws CelestaException {
    String sql = String.format(
        "select OBJECT_NAME from all_objects\n" +
            " where owner = sys_context('userenv','session_user')\n" +
            " and object_type = 'FUNCTION' and object_name like '%s@_%%' escape '@'",
        g.getName());
    List<String> result = new LinkedList<>();
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        String buf = rs.getString(1);
        Matcher m = TABLE_PATTERN.matcher(buf);
        m.find();
        result.add(m.group(2));
      }
    } catch (SQLException e) {
      throw new CelestaException("Cannot get views list: %s", e.toString());
    }
    return result;
  }

  @Override
  public void dropParameterizedView(Connection conn, String grainName, String viewName) throws CelestaException {
    //удалить pview
    try (Statement stmt = conn.createStatement()) {
      String sql = String.format("DROP FUNCTION " + tableTemplate(), grainName, viewName);
      stmt.executeUpdate(sql);

      ResultSet rs;

      //удалить табличный тип
      sql = String.format(
          "select TYPE_NAME from DBA_TYPES WHERE owner = sys_context('userenv','session_user')\n" +
              " and TYPECODE = 'COLLECTION' and TYPE_NAME = '%s_%s_t'",
          grainName, viewName);
      rs = stmt.executeQuery(sql);

      if (rs.next()) {
        sql = String.format(
            "DROP TYPE \"%s_%s_t\"",
            grainName, viewName);
        stmt.executeUpdate(sql);
      }

      //удалить объект записи
      sql = String.format(
          "select TYPE_NAME from DBA_TYPES WHERE owner = sys_context('userenv','session_user')\n" +
              " and TYPECODE = 'OBJECT' and TYPE_NAME = '%s_%s_o'",
          grainName, viewName);
      rs = stmt.executeQuery(sql);

      if (rs.next()) {
        sql = String.format(
            "DROP TYPE \"%s_%s_o\"",
            grainName, viewName);
        stmt.executeUpdate(sql);
      }

      conn.commit();
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  @Override
  public String getCallFunctionSql(ParameterizedView pv) throws CelestaException {
    return String.format(
        "TABLE(" + tableTemplate() + "(%s))",
        pv.getGrain().getName(), pv.getName(),
        pv.getParameters().keySet().stream()
            .map(p -> "?")
            .collect(Collectors.joining(", "))
    );
  }

  @Override
  public void createParameterizedView(Connection conn, ParameterizedView pv) throws CelestaException {

    try (Statement stmt = conn.createStatement()) {
      //Создаем тип
      String colsDef = pv.getColumns().entrySet().stream()
          .map(e -> {
            StringBuilder sb = new StringBuilder("\"").append(e.getKey()).append("\" ")
                .append(TYPES_DICT.get(
                    CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getCelestaType())
                ).dbFieldType());

            Column colRef = pv.getColumnRef(e.getKey());

            if (colRef != null && StringColumn.VARCHAR.equals(colRef.getCelestaType())) {
              StringColumn sc = (StringColumn) colRef;
              sb.append("(").append(sc.getLength()).append(")");
            }

            return sb.toString();
          }).collect(Collectors.joining(",\n"));

      String sql = String.format("create type " + tableTemplate() + " as object\n" +
          "(%s)", pv.getGrain().getName(), pv.getName() + "_o", colsDef);
      //System.out.println(sql);
      stmt.executeUpdate(sql);

      //Создаем коллекцию типов
      sql = String.format("create type " + tableTemplate() + " as TABLE OF " + tableTemplate(),
          pv.getGrain().getName(), pv.getName() + "_t", pv.getGrain().getName(), pv.getName() + "_o");
      //System.out.println(sql);
      stmt.executeUpdate(sql);

      //Создаем функцию
      SQLGenerator gen = getViewSQLGenerator();
      StringWriter sw = new StringWriter();
      BufferedWriter bw = new BufferedWriter(sw);

      pv.selectScript(bw, gen);
      bw.flush();

      String pvParams = pv.getParameters()
          .entrySet().stream()
          .map(e ->
              e.getKey() + " IN "
                  + TYPES_DICT.get(
                  CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getType().getCelestaType())
              ).dbFieldType()

          ).collect(Collectors.joining(", "));

      String selectSql = sw.toString();

      String objectParams = pv.getColumns().keySet().stream()
          .map(alias -> "curr.\"" + alias + "\"")
          .collect(Collectors.joining(", "));

      sql = String.format(
          "create or replace function " + tableTemplate() + "(%s) return " + tableTemplate()
              + " PIPELINED IS\n"
              + "BEGIN\n"
              + "for curr in (%s) loop \n"
              + "pipe row (%s(%s));\n"
              + "end loop;"
              + "END;",
          pv.getGrain().getName(), pv.getName(), pvParams,
          pv.getGrain().getName(), pv.getName() + "_t",
          selectSql, String.format(tableTemplate(), pv.getGrain().getName(), pv.getName() + "_o"),
          objectParams);

      //System.out.println(sql);
      stmt.executeUpdate(sql);
    } catch (SQLException | IOException e) {
      e.printStackTrace();
      throw new CelestaException("Error while creating parameterized view %s.%s: %s",
          pv.getGrain().getName(), pv.getName(), e.getMessage());
    }
  }

  @Override
  public SQLGenerator getViewSQLGenerator() {
    return new SQLGenerator() {

      @Override
      protected String viewName(AbstractView v) {
        return String.format(TABLE_TEMPLATE, v.getGrain().getName(), v.getName());
      }

      @Override
      protected String tableName(TableRef tRef) {
        Table t = tRef.getTable();
        return String.format(TABLE_TEMPLATE + " \"%s\"", t.getGrain().getName(), t.getName(), tRef.getAlias());
      }

      @Override
      protected String checkForDate(String lexValue) {
        try {
          return translateDate(lexValue);
        } catch (CelestaException e) {
          // This is not a date
          return lexValue;
        }
      }

      @Override
      protected String boolLiteral(boolean val) {
        return val ? "1" : "0";
      }

      @Override
      protected String paramLiteral(String paramName) {
        return paramName;
      }

      @Override
      protected String getDate() {
        return "CURRENT_TIMESTAMP";
      }
    };
  }

  private static String getUpdTriggerName(TableElement table) {
    String result = String.format("%s_%s_upd", table.getGrain().getName(), table.getName());
    result = NamedElement.limitName(result);
    return result;
  }

  private void dropVersioningTrigger(Connection conn, TableElement t) throws CelestaException {
    // First of all, we are about to check if trigger exists
    String triggerName = getUpdTriggerName(t);
    TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
        .withName(triggerName)
        .withTableName(t.getName());

    try {
      boolean triggerExists = triggerExists(conn, query);

      if (triggerExists)
        dropTrigger(conn, query);

    } catch (SQLException e) {
      throw new CelestaException("Could not drop version check trigger on %s.%s: %s", t.getGrain().getName(),
          t.getName(), e.getMessage());
    }
  }

  @Override
  public boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException {
    String sql = String.format(
        SELECT_TRIGGER_BODY
            + "and table_name = '%s_%s' and trigger_name = '%s' and triggering_event = '%s'",
        query.getSchema(), query.getTableName(), query.getName(),
        TRIGGER_EVENT_TYPE_DICT.get(query.getType()));

    Statement stmt = conn.createStatement();
    try {
      ResultSet rs = stmt.executeQuery(sql);
      boolean result = rs.next();
      rs.close();
      return result;
    } finally {
      stmt.close();
    }
  }

  @Override
  public void dropTrigger(Connection conn, TriggerQuery query) throws SQLException {
    Statement stmt = conn.createStatement();

    try {
      String sql = String.format(DROP_TRIGGER + "%s\"", query.getName());
      stmt.executeUpdate(sql);
    } finally {
      stmt.close();
    }
  }

  @Override
  public void updateVersioningTrigger(Connection conn, TableElement t) throws CelestaException {
    // First of all, we are about to check if trigger exists
    String triggerName = getUpdTriggerName(t);

    try {
      Statement stmt = conn.createStatement();
      try {
        TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
            .withName(triggerName)
            .withTableName(t.getName())
            .withType(TriggerType.PRE_UPDATE);
        boolean triggerExists = triggerExists(conn, query);

        if (t instanceof VersionedElement) {
          VersionedElement ve = (VersionedElement) t;

          String sql;
          if (ve.isVersioned()) {
            if (triggerExists) {
              return;
            } else {
              // CREATE TRIGGER
              sql = String.format("CREATE OR REPLACE TRIGGER \"%s\" BEFORE UPDATE ON \"%s_%s\" FOR EACH ROW\n"
                      + "BEGIN\n" + "  IF :new.\"recversion\" <> :old.\"recversion\" THEN\n"
                      + "    raise_application_error( -20001, 'record version check failure' );\n"
                      + "  END IF;\n" + "  :new.\"recversion\" := :new.\"recversion\" + 1;\n" + "END;",
                  triggerName, t.getGrain().getName(), t.getName());
              // System.out.println(sql);
              stmt.executeUpdate(sql);
            }
          } else {
            if (triggerExists) {
              // DROP TRIGGER
              TriggerQuery dropQuery = new TriggerQuery().withName(triggerName);
              dropTrigger(conn, dropQuery);
            } else {
              return;
            }
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
  public PreparedStatement getNavigationStatement(
      Connection conn, FromClause from, String orderBy,
      String navigationWhereClause, Set<String> fields, long offset
  ) throws CelestaException {
    if (navigationWhereClause == null)
      throw new IllegalArgumentException();

    StringBuilder w = new StringBuilder(navigationWhereClause);
    final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);

    final String sql;

    if (offset == 0) {
      if (orderBy.length() > 0)
        w.append(" order by " + orderBy);

        sql = String.format(SELECT_S_FROM
                      + " (" + SELECT_S_FROM + " %s  %s)"
                      + " where rownum = 1", fieldList, fieldList,
              from.getExpression(), "where " + w);
    } else {
      sql = getLimitedSqlWithOffset(orderBy, fields, from, w.toString(), offset - 1, offset);
    }

    // System.out.println(sql);
    return prepareStatement(conn, sql);
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

  @Override
  public void resetIdentity(Connection conn, Table t, int i) throws SQLException {
    String sequenceName = getSequenceName(t);
    Statement stmt = conn.createStatement();
    try {
      String sql = String.format("select \"%s\".nextval from dual", sequenceName);
      ResultSet rs = stmt.executeQuery(sql);
      rs.next();
      int curVal = rs.getInt(1);
      rs.close();
      sql = String.format("alter sequence \"%s\" increment by %d minvalue 1", sequenceName, i - curVal - 1);
      stmt.executeUpdate(sql);
      sql = String.format("select \"%s\".nextval from dual", sequenceName);
      stmt.executeQuery(sql).close();
      sql = String.format("alter sequence \"%s\" increment by 1 minvalue 1", sequenceName);
      stmt.executeUpdate(sql);
    } finally {
      stmt.close();
    }
  }


  @Override
  public int getDBPid(Connection conn) {
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("select sys_context('userenv','sessionid') from dual");
      if (rs.next())
        return rs.getInt(1);
    } catch (SQLException e) {
      // do nothing
    }
    return 0;
  }


  @Override
  public boolean nullsFirst() {
    return false;
  }

  @Override
  public void createTableTriggersForMaterializedViews(Connection conn, Table t) throws CelestaException {

    List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
        .filter(mv -> mv.getRefTable().getTable().equals(t))
        .collect(Collectors.toList());

    String fullTableName = String.format(tableTemplate(), t.getGrain().getName(), t.getName());

    for (MaterializedView mv : mvList) {
      String fullMvName = String.format(tableTemplate(), mv.getGrain().getName(), mv.getName());

      String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
      String updateTriggerName = mv.getTriggerName(TriggerType.POST_UPDATE);
      String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

      String lockTable = String.format("LOCK TABLE %s IN EXCLUSIVE MODE;\n", fullMvName);

      String mvColumns = mv.getColumns().keySet().stream()
          .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
          .map(alias -> "\"" + alias + "\"")
          .collect(Collectors.joining(", "));

      String selectFromRowTemplate = mv.getColumns().keySet().stream()
          .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
          .map(alias -> {
            Column colRef = mv.getColumnRef(alias);

            if (colRef == null) {
              Map<String, Expr> aggrCols = mv.getAggregateColumns();
              if (aggrCols.containsKey(alias) && aggrCols.get(alias) instanceof Count) {
                return "1 as \"" + alias + "\"";
              }
              return "";
            }

            if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
              return "TRUNC(%1$s.\"" + mv.getColumnRef(alias).getName() + "\", 'DD') as \"" + alias + "\"";
            }

            return "%1$s.\"" + mv.getColumnRef(alias).getName() + "\" as \"" + alias + "\"";
          })
          .filter(str -> !str.isEmpty())
          .collect(Collectors.joining(", "));


      String rowColumnsTemplate = mv.getColumns().keySet().stream()
          .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
          .map(alias -> "%1$s.\"" + alias + "\"")
          .collect(Collectors.joining(", "));

      String rowConditionTemplate = mv.getColumns().keySet().stream()
          .filter(alias -> mv.isGroupByColumn(alias))
          .map(alias -> "mv.\"" + alias + "\" = %1$s.\"" + alias + "\"")
          .collect(Collectors.joining(" AND "));

      String rowConditionTemplateForDelete = mv.getColumns().keySet().stream()
          .filter(alias -> mv.isGroupByColumn(alias))
          .map(alias -> {
            Column colRef = mv.getColumnRef(alias);

            if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
              return "mv.\"" + alias + "\" = TRUNC(%1$s.\"" + mv.getColumnRef(alias).getName() + "\", 'DD')";
            }

            return "mv.\"" + alias + "\" = %1$s.\"" + mv.getColumnRef(alias).getName() + "\"";
          })
          .collect(Collectors.joining(" AND "));

      String setStatementTemplate = mv.getAggregateColumns().entrySet().stream()
          .map(e -> {
            StringBuilder sb = new StringBuilder();
            String alias = e.getKey();

            sb.append("mv.\"").append(alias)
                .append("\" = mv.\"").append(alias)
                .append("\" %1$s ");

            if (e.getValue() instanceof Sum) {
              sb.append("%2$s.\"").append(alias).append("\"");
            } else if (e.getValue() instanceof Count) {
              sb.append("1");
            }

            return sb.toString();
          }).collect(Collectors.joining(", "))
          .concat(", mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" = ")
          .concat("mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" %1$s 1");


      String setStatementTemplateForDelete = mv.getAggregateColumns().entrySet().stream()
          .map(e -> {
            StringBuilder sb = new StringBuilder();
            String alias = e.getKey();

            sb.append("mv.\"").append(alias)
                .append("\" = mv.\"").append(alias)
                .append("\" %1$s ");

            if (e.getValue() instanceof Sum) {
              sb.append("%2$s.\"").append(mv.getColumnRef(alias).getName()).append("\"");
            } else if (e.getValue() instanceof Count) {
              sb.append("1");
            }

            return sb.toString();
          }).collect(Collectors.joining(", "))
          .concat(", mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" = ")
          .concat("mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" %1$s 1");


      StringBuilder insertSqlBuilder = new StringBuilder("MERGE INTO %s mv \n")
          .append("USING (SELECT %s FROM dual) \"inserted\" ON (%s) \n")
          .append("WHEN MATCHED THEN \n ")
          .append("UPDATE SET %s \n")
          .append("WHEN NOT MATCHED THEN \n")
          .append("INSERT (%s) VALUES (%s); \n");

      String insertSql = String.format(insertSqlBuilder.toString(), fullMvName,
          String.format(selectFromRowTemplate, ":new"), String.format(rowConditionTemplate, "\"inserted\""),
          String.format(setStatementTemplate, "+", "\"inserted\""),
          mvColumns + ", \"" + MaterializedView.SURROGATE_COUNT + "\"",
          String.format(rowColumnsTemplate, "\"inserted\"") + ", 1");

      String delStatement = String.format("mv.\"%s\" = 0", MaterializedView.SURROGATE_COUNT);

      StringBuilder deleteSqlBuilder = new StringBuilder(String.format("UPDATE %s mv \n", fullMvName))
          .append("SET ").append(String.format(setStatementTemplateForDelete, "-", ":old")).append(" ")
          .append("WHERE ").append(String.format(rowConditionTemplateForDelete, ":old")).append(";\n")
          .append(String.format("DELETE FROM %s mv ", fullMvName))
          .append("WHERE ").append(delStatement).append(";\n");


      String sql;
      try (Statement stmt = conn.createStatement()) {
        //INSERT
        try {
          sql = String.format("create or replace trigger \"%s\" after insert " +
                  "on %s for each row\n"
                  + "begin \n" + MaterializedView.CHECKSUM_COMMENT_TEMPLATE
                  + "\n %s \n %s \n END;",
              insertTriggerName, fullTableName, mv.getChecksum(), lockTable, insertSql);
          //System.out.println(sql);
          stmt.execute(sql);
        } catch (SQLException e) {
          throw new CelestaException("Could not update insert-trigger on %s for materialized view %s: %s",
              fullTableName, fullMvName, e);
        }
        //UPDATE
        try {
          sql = String.format("create or replace trigger \"%s\" after update " +
                  "on %s for each row\n"
                  + "begin %s \n %s\n %s\n END;",
              updateTriggerName, fullTableName, lockTable, deleteSqlBuilder.toString(), insertSql);

          //System.out.println(sql);
          stmt.execute(sql);
        } catch (SQLException e) {
          throw new CelestaException("Could not update update-trigger on %s for materialized view %s: %s",
              fullTableName, fullMvName, e);
        }
        //DELETE
        try {
          sql = String.format("create or replace trigger \"%s\" after delete " +
                  "on %s for each row\n "
                  + " begin %s \n %s\n END;",
              deleteTriggerName, fullTableName, lockTable, deleteSqlBuilder.toString());

          stmt.execute(sql);
        } catch (SQLException e) {
          throw new CelestaException("Could not update update-trigger on %s for materialized view %s: %s",
              fullTableName, fullMvName, e);
        }
      } catch (SQLException e) {
        throw new CelestaException("Could not update triggers on %s for materialized view %s: %s",
            fullTableName, fullMvName, e);
      }
    }
  }

  @Override
  public void dropTableTriggersForMaterializedViews(Connection conn, Table t) throws CelestaException {
    List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
        .filter(mv -> mv.getRefTable().getTable().equals(t))
        .collect(Collectors.toList());

    for (MaterializedView mv : mvList) {
      TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
          .withTableName(t.getName());

      String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
      String updateTriggerName = mv.getTriggerName(TriggerType.POST_UPDATE);
      String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

      try {
        query.withName(insertTriggerName);
        if (triggerExists(conn, query))
          dropTrigger(conn, query);
        query.withName(updateTriggerName);
        if (triggerExists(conn, query))
          dropTrigger(conn, query);
        query.withName(deleteTriggerName);
        if (triggerExists(conn, query))
          dropTrigger(conn, query);
      } catch (SQLException e) {
        throw new CelestaException("Can't drop triggers for materialized view %s.%s: %s",
            mv.getGrain().getName(), mv.getName(), e.getMessage());
      }
    }
  }


  @Override
  String getSelectTriggerBodySql(TriggerQuery query) {
    String sql = String.format(SELECT_TRIGGER_BODY + "and table_name = '%s_%s' and trigger_name = '%s'",
        query.getSchema(), query.getTableName(), query.getName());

    return sql;
  }

  @Override
  String truncDate(String dateStr) {
    return "TRUNC(" + dateStr + " , 'DD')";
  }

  @Override
  String constantFromSql() {
    return "FROM DUAL";
  }

  @Override
  public AppSettings.DBType getType() {
    return AppSettings.DBType.ORACLE;
  }
}
