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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.DBType;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.adaptors.ddl.*;

import static ru.curs.celesta.dbutils.adaptors.constants.OraConstants.*;
import static ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions.*;

import ru.curs.celesta.dbutils.adaptors.function.OraFunctions;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;
import ru.curs.celesta.score.validator.AnsiQuotedIdentifierParser;

import static ru.curs.celesta.dbutils.jdbc.SqlUtils.*;

/**
 * Oracle Database Adaptor.
 */
public final class OraAdaptor extends DBAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OraAdaptor.class);

    private static final String SELECT_S_FROM = "select %s from ";

    private static final String SELECT_TRIGGER_BODY = "select TRIGGER_BODY  from all_triggers "
            + "where owner = sys_context('userenv','session_user') ";

    private static final Pattern BOOLEAN_CHECK = Pattern.compile("\"([^\"]+)\" *[iI][nN] *\\( *0 *, *1 *\\)");
    private static final Pattern DATE_PATTERN = Pattern.compile("'(\\d\\d\\d\\d)-([01]\\d)-([0123]\\d)'");
    private static final Pattern HEX_STRING = Pattern.compile("'([0-9A-F]+)'");
    private static final Pattern TABLE_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");

    private static final Map<TriggerType, String> TRIGGER_EVENT_TYPE_DICT = new HashMap<>();


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

    public OraAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new OraDdlGenerator(this);
    }

    @Override
    public boolean tableExists(Connection conn, String schema, String name) {
        if (schema == null || schema.isEmpty() || name == null || name.isEmpty()) {
            return false;
        }
        String sql = String.format("select count(*) from all_tables where owner = "
                        + "sys_context('userenv','session_user') and table_name = '%s_%s'",
                schema, name);

        try (Statement checkForTable = conn.createStatement();
             ResultSet rs = checkForTable.executeQuery(sql)
        ) {
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }


    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM USER_TABLES");
             ResultSet rs = pstmt.executeQuery()) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    @Override
    void createSchemaIfNotExists(Connection conn, String schema) {
        ddlAdaptor.createSchema(conn, schema);
    }

    @Override
    public PreparedStatement getOneFieldStatement(Connection conn, Column<?> c, String where) {
        TableElement t = c.getParentTable();
        String sql = String.format(SELECT_S_FROM + tableString(t.getGrain().getName(), t.getName())
                + " where %s and rownum = 1", c.getQuotedName(), where);
        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement getOneRecordStatement(
            Connection conn, TableElement t, String where, Set<String> fields
    ) {

        final String fieldList = getTableFieldsListExceptBlobs((DataGrainElement) t, fields);

        String sql = String.format(SELECT_S_FROM + tableString(t.getGrain().getName(), t.getName())
                + " where %s and rownum = 1", fieldList, where);
        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement getInsertRecordStatement(Connection conn, BasicTable t, boolean[] nullsMask,
                                                      List<ParameterSetter> program) {

        Iterator<String> columns = t.getColumns().keySet().iterator();
        // Создаём параметризуемую часть запроса, пропуская нулевые значения.
        StringBuilder fields = new StringBuilder();
        StringBuilder params = new StringBuilder();
        for (int i = 0; i < t.getColumns().size(); i++) {
            String c = columns.next();
            if (nullsMask[i]) {
                continue;
            }
            if (params.length() > 0) {
                fields.append(", ");
                params.append(", ");
            }
            params.append("?");
            fields.append('"');
            fields.append(c);
            fields.append('"');
            program.add(ParameterSetter.create(i, this));
        }

        final String sql;

        if (fields.length() == 0 && params.length() == 0) {
            //Для выполнения пустого insert ищем любое поле, отличное от recversion
            String columnToInsert = t.getColumns().keySet()
                    .stream()
                    .filter(k -> !VersionedElement.REC_VERSION.equals(k))
                    .findFirst().get();

            sql = String.format(
                    "insert into " + tableString(t.getGrain().getName(), t.getName())
                            + " (\"%s\") values (DEFAULT)", columnToInsert
            );
        } else {
            sql = String.format(
                    "insert into " + tableString(t.getGrain().getName(), t.getName())
                            + " (%s) values (%s)", fields.toString(), params.toString()
            );
        }
        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) {
        String sql = String.format("delete " + tableString(t.getGrain().getName(), t.getName()) + " where %s", where);
        return prepareStatement(conn, sql);
    }

    @Override
    public Set<String> getColumns(Connection conn, TableElement t) {
        Set<String> result = new LinkedHashSet<>();
        try {
            String tableName = String.format("%s_%s", t.getGrain().getName(), t.getName());
            String sql = String.format(
                    "SELECT column_name FROM user_tab_cols WHERE table_name = '%s' order by column_id", tableName);
            LOGGER.trace(sql);
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
    public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) {
        String sql = String.format("delete from " + tableString(t.getGrain().getName(), t.getName()) + " %s",
                where.isEmpty() ? "" : "where " + where);
        try {
            PreparedStatement result = conn.prepareStatement(sql);
            return result;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public boolean isValidConnection(Connection conn, int timeout) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 FROM Dual")) {
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public String tableString(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder(getSchemaUnderscoreNameTemplate(schemaName, tableName));
        sb.insert(0, '"').append('"');

        return sb.toString();
    }

    private String getSchemaUnderscoreNameTemplate(String schemaName, String name) {
        return stripNameFromQuotes(schemaName) + "_" + stripNameFromQuotes(name);
    }

    private String stripNameFromQuotes(String name) {
        return name.startsWith("\"") ? name.substring(1, name.length() - 1) : name;
    }

    @Override
    public String sequenceString(String schemaName, String sequenceName) {
        return sequenceString(schemaName, sequenceName, true);
    }

    private String sequenceString(String schemaName, String sequenceName, boolean isQuoted) {
        StringBuilder sb = new StringBuilder(NamedElement.limitName(
                getSchemaUnderscoreNameTemplate(schemaName, sequenceName)));
        if (isQuoted) {
            sb.insert(0, '"').append('"');
        }

        return sb.toString();
    }

    @Override
    public String pkConstraintString(TableElement tableElement) {
        return NamedElement.limitName(
                tableElement.getPkConstraintName() + "_" + tableElement.getGrain().getName());
    }

    public int getCurrentIdent(Connection conn, BasicTable t) {
        final String sequenceName;

        IntegerColumn idColumn = t.getPrimaryKey().values().stream()
                .filter(c -> c instanceof IntegerColumn)
                .map(c -> (IntegerColumn) c)
                .filter(ic -> ic.getSequence() != null)
                .findFirst().get();

        sequenceName = tableString(t.getGrain().getName(), idColumn.getSequence().getName());

        String sql = String.format("SELECT %s.CURRVAL FROM DUAL", sequenceName);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields,
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

        String otherTableStr = tableString(otherDge.getGrain().getName(), otherDge.getName());
        String result = String.format(template, fieldsStr, otherFieldsStr, otherTableStr, otherWhere);
        return result;
    }

    private boolean checkForBoolean(Connection conn, Column<?> c) throws SQLException {
        String sql = String.format(
                "SELECT SEARCH_CONDITION FROM ALL_CONSTRAINTS WHERE " + "OWNER = sys_context('userenv','session_user')"
                        + " AND TABLE_NAME = '%s_%s'" + "AND CONSTRAINT_TYPE = 'C'",
                c.getParentTable().getGrain().getName(), c.getParentTable().getName());
        LOGGER.trace(sql);
        PreparedStatement checkForBool = conn.prepareStatement(sql);
        try {
            ResultSet rs = checkForBool.executeQuery();
            while (rs.next()) {
                String buf = rs.getString(1);
                Matcher m = BOOLEAN_CHECK.matcher(buf);
                if (m.find() && m.group(1).equals(c.getName())) {
                    return true;
                }
            }
        } finally {
            checkForBool.close();
        }
        return false;

    }

    @Override
    public DbColumnInfo getColumnInfo(Connection conn, Column<?> c) {
        try {
            String tableName = String.format("%s_%s", c.getParentTable().getGrain().getName(),
                    c.getParentTable().getName());
            String sql = String.format(
                    "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE "
                            + "FROM user_tab_cols    WHERE table_name = '%s' and COLUMN_NAME = '%s'",
                    tableName, c.getName());
            LOGGER.trace(sql);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            DbColumnInfo result;
            try {
                if (rs.next()) {
                    result = new DbColumnInfo();
                    result.setName(rs.getString(COLUMN_NAME));
                    String typeName = rs.getString("DATA_TYPE");

                    if (typeName.startsWith("TIMESTAMP")) {
                        if (typeName.endsWith("WITH TIME ZONE")) {
                            result.setType(ZonedDateTimeColumn.class);
                        } else {
                            result.setType(DateTimeColumn.class);
                        }
                    } else if ("float".equalsIgnoreCase(typeName)) {
                        result.setType(FloatingColumn.class);
                    } else if ("nclob".equalsIgnoreCase(typeName)) {
                        result.setType(StringColumn.class);
                        result.setMax(true);
                    } else if ("number".equalsIgnoreCase(typeName)
                            && rs.getInt("DATA_PRECISION") != 0 && rs.getInt("DATA_SCALE") != 0) {
                        result.setType(DecimalColumn.class);
                        result.setLength(rs.getInt("DATA_PRECISION"));
                        result.setScale(rs.getInt("DATA_SCALE"));
                    } else {
                        for (Class<? extends Column<?>> cc : COLUMN_CLASSES) {
                            if (getColumnDefiner(cc).dbFieldType().equalsIgnoreCase(typeName)) {
                                result.setType(cc);
                                break;
                            }
                        }
                    }
                    if (IntegerColumn.class == result.getType()) {
                        // В Oracle булевские столбцы имеют тот же тип данных,
                        // что и INT-столбцы: просматриваем, есть ли на них
                        // ограничение CHECK.
                        if (checkForBoolean(conn, c)) {
                            result.setType(BooleanColumn.class);
                        }
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

    private void processDefaults(Connection conn, Column<?> c, DbColumnInfo result) throws SQLException {
        ResultSet rs;
        TableElement te = c.getParentTable();
        Grain g = te.getGrain();
        PreparedStatement getDefault = conn.prepareStatement(String.format(
                "select DATA_DEFAULT from DBA_TAB_COLUMNS where " + "owner = sys_context('userenv','session_user') "
                        + "and TABLE_NAME = '%s_%s' and COLUMN_NAME = '%s'",
                g.getName(), te.getName(), c.getName()));
        try {
            rs = getDefault.executeQuery();
            if (!rs.next()) {
                return;
            }
            String body = rs.getString(1);
            if (body == null || "null".equalsIgnoreCase(body)) {

                if (c instanceof IntegerColumn) {
                    IntegerColumn ic = (IntegerColumn) c;
                    String sequenceTriggerName = generateSequenceTriggerName(ic);

                    String sql = String.format(
                            "SELECT REFERENCED_NAME FROM USER_DEPENDENCIES "
                         + " WHERE NAME = '%s' "
                              + " AND TYPE = 'TRIGGER' "
                              + " AND REFERENCED_TYPE = 'SEQUENCE'",
                                               sequenceTriggerName);

                    try (Statement stmt = conn.createStatement();
                         ResultSet sequenceRs = stmt.executeQuery(sql)) {
                        if (sequenceRs.next()) {
                            String sequenceName = sequenceRs.getString(1);
                            body = "NEXTVAL(" + sequenceName.replace(g.getName() + "_", "") + ")";
                        } else {
                            return;
                        }
                    }

                } else {
                    return;
                }
            }
            if (BooleanColumn.class == result.getType()) {
                body = "0".equals(body.trim()) ? "'FALSE'" : "'TRUE'";
            } else if (DateTimeColumn.class == result.getType()) {
                if (body.toLowerCase().contains("sysdate")) {
                    body = "GETDATE()";
                } else {
                    Matcher m = DATE_PATTERN.matcher(body);
                    if (m.find()) {
                        body = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
                    }
                }
            } else if (BinaryColumn.class == result.getType()) {
                Matcher m = HEX_STRING.matcher(body);
                if (m.find()) {
                    body = "0x" + m.group(1);
                }
            } else {
                body = body.trim();
            }
            result.setDefaultValue(body);

        } finally {
            getDefault.close();
        }
    }

    //TODO:must be defined in single place
    private static String getFKTriggerName(String prefix, String fkName) {
        String result = prefix + fkName;
        result = NamedElement.limitName(result);
        return result;
    }

    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        DbPkInfo result = new DbPkInfo(this);
        try {
            String sql = String.format("select cons.constraint_name, column_name from all_constraints cons "
                            + "inner join all_cons_columns cols on cons.constraint_name = cols.constraint_name  "
                            + "and cons.owner = cols.owner where "
                            + "cons.owner = sys_context('userenv','session_user') "
                            + "and cons.table_name = '%s_%s'"
                            + " and cons.constraint_type = 'P' order by cols.position",
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
    public List<DbFkInfo> getFKInfo(Connection conn, Grain g) {
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

        List<DbFkInfo> result = new LinkedList<>();
        try {
            Statement stmt = conn.createStatement();
            try {
                DbFkInfo i = null;
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String fkName = rs.getString("CONSTRAINT_NAME");
                    if (i == null || !i.getName().equals(fkName)) {
                        i = new DbFkInfo(fkName);
                        result.add(i);
                        String tableName = rs.getString("TABLE_NAME");
                        i.setTableName(convertNameFromDb(tableName, g));
                        tableName = rs.getString("REF_TABLE_NAME");
                        i.setRefGrainName(tableName.substring(0, tableName.indexOf("_")));
                        i.setRefTableName(tableName.substring(tableName.indexOf("_") + 1));
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
                if (sql.startsWith(CSC)) {
                    return FKRule.CASCADE;
                } else if (sql.startsWith(SNL)) {
                    return FKRule.SET_NULL;
                }
            }
            return FKRule.NO_ACTION;
        } finally {
            stmt.close();
        }
    }

    @Override
    String getLimitedSQL(
            FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields
    ) {
        if (offset == 0 && rowCount == 0) {
            throw new IllegalArgumentException();
        }
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

    private String getLimitedSqlWithOffset(String orderBy, Set<String> fields, FromClause from, String where,
                                           long offset, long rowCount) {
        return String.format(
                "with a as (%s) select * from (select a.*, ROWNUM rnum "
                        + "from a where rownum <= %d) where rnum >= %d order by rnum",
                getSelectFromOrderBy(from, where, orderBy, fields), offset + rowCount, offset + 1L);
    }

    @Override
    public Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) {
        String sql = String
                .format("select ind.table_name TABLE_NAME, ind.index_name INDEX_NAME, cols.column_name COLUMN_NAME,"
                        + " cols.column_position POSITION " + "from all_indexes ind "
                        + "inner join all_ind_columns cols " + "on ind.owner = cols.index_owner "
                        + "and ind.table_name = cols.table_name " + "and ind.index_name = cols.index_name "
                        + "where ind.owner = sys_context('userenv','session_user') and ind.uniqueness = 'NONUNIQUE' "
                        + "and ind.table_name like '%s@_%%' escape '@'"
                        + "order by ind.table_name, ind.index_name, cols.column_position", g.getName());

        Map<String, DbIndexInfo> result = new HashMap<>();
        try {
            Statement stmt = conn.createStatement();
            try {
                DbIndexInfo i = null;
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String tabName = rs.getString("TABLE_NAME");
                    tabName = convertNameFromDb(tabName, g);
                    String dbIndName = rs.getString("INDEX_NAME");
                    final String indName;

                    if (convertNameFromDb(dbIndName, g) != null) {
                        indName = convertNameFromDb(dbIndName, g);
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
                        indName = "##" + dbIndName;
                    }

                    if (i == null || !i.getTableName().equals(tabName) || !i.getIndexName().equals(indName)) {
                        i = new DbIndexInfo(tabName, indName);
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
    public List<String> getViewList(Connection conn, Grain g) {
        String sql = String.format(
                "select view_name from all_views "
                        + "where owner = sys_context('userenv','session_user') and view_name like '%s@_%%' escape '@'",
                g.getName());
        List<String> result = new LinkedList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String dbName = rs.getString(1);
                result.add(convertNameFromDb(dbName, g));
            }
        } catch (SQLException e) {
            throw new CelestaException("Cannot get views list: %s", e.toString());
        }
        return result;
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        String sql = String.format(
                "select OBJECT_NAME from all_objects\n"
             + " where owner = sys_context('userenv','session_user')\n"
                 + " and object_type = 'FUNCTION' and object_name like '%s@_%%' escape '@'",
                g.getName());
        List<String> result = new LinkedList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String dbName = rs.getString(1);
                result.add(convertNameFromDb(dbName, g));
            }
        } catch (SQLException e) {
            throw new CelestaException("Cannot get views list: %s", e.toString());
        }
        return result;
    }

    @Override
    public String getCallFunctionSql(ParameterizedView pv) {
        return String.format(
                "TABLE(" + tableString(pv.getGrain().getName(), pv.getName()) + "(%s))",
                pv.getParameters().keySet().stream()
                        .map(p -> "?")
                        .collect(Collectors.joining(", "))
        );
    }

    @Override
    public boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException {
        String sql = String.format(
                SELECT_TRIGGER_BODY
                        + "and table_name = '%s_%s' and trigger_name = '%s' and triggering_event = '%s'",
                query.getSchema(),
                query.getTableName(),
                query.getName(),
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
    public PreparedStatement getNavigationStatement(
            Connection conn, FromClause from, String orderBy,
            String navigationWhereClause, Set<String> fields, long offset
    ) {
        if (navigationWhereClause == null) {
            throw new IllegalArgumentException();
        }

        StringBuilder w = new StringBuilder(navigationWhereClause);
        final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);

        final String sql;

        if (offset == 0) {
            if (orderBy.length() > 0) {
                w.append(" order by " + orderBy);
            }

            sql = String.format(SELECT_S_FROM
                            + " (" + SELECT_S_FROM + " %s  %s)"
                            + " where rownum = 1", fieldList, fieldList,
                    from.getExpression(), "where " + w);
        } else {
            sql = getLimitedSqlWithOffset(orderBy, fields, from, w.toString(), offset - 1, offset);
        }

        LOGGER.trace(sql);
        return prepareStatement(conn, sql);
    }

    @Override
    public String translateDate(String date) {
        return OraFunctions.translateDate(date);
    }

    @Override
    public int getDBPid(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select sys_context('userenv','sessionid') from dual")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            // do nothing
        }
        return 0;
    }

    @Override
    public ZonedDateTime prepareZonedDateTimeForParameterSetter(Connection conn, ZonedDateTime z) {

        try (ResultSet rs = executeQuery(conn, "SELECT SESSIONTIMEZONE FROM DUAL")) {
            rs.next();
            String zoneId = rs.getString(1);

            Instant instant = Instant.now();
            ZoneId systemZone = ZoneId.of(zoneId);
            ZoneOffset systemOffset = systemZone.getRules().getOffset(instant);
            int offsetDifInSeconds = systemOffset.getTotalSeconds();

            return z.plusSeconds(offsetDifInSeconds);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public boolean nullsFirst() {
        return false;
    }


    @Override
    String getSelectTriggerBodySql(TriggerQuery query) {
        String sql = String.format(SELECT_TRIGGER_BODY + "and table_name = '%s_%s' and trigger_name = '%s'",
                query.getSchema(), query.getTableName(), query.getName());

        return sql;
    }

    @Override
    String constantFromSql() {
        return "FROM DUAL";
    }

    @Override
    public DBType getType() {
        return DBType.ORACLE;
    }

    @Override
    public long nextSequenceValue(Connection conn, SequenceElement s) {
        String sql = "SELECT " + sequenceString(s.getGrain().getName(), s.getName()) + ".nextval from DUAL";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new CelestaException(
                    "Can't get current value of sequence " + tableString(s.getGrain().getName(), s.getName()), e
            );
        }
    }

    @Override
    public boolean sequenceExists(Connection conn, final String schema, final String name) {
        String sql = String.format(
                "select count(*) from user_sequences where sequence_name = '%s'",
                sequenceString(schema, name, false)
        );

        try (Statement checkForTable = conn.createStatement();
             ResultSet rs = checkForTable.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        String sql = "SELECT INCREMENT_BY, MIN_VALUE, MAX_VALUE, CYCLE_FLAG"
                  + " FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, sequenceString(s.getGrain().getName(), s.getName(), false));
            try (ResultSet rs = preparedStatement.executeQuery()) {
                rs.next();
                DbSequenceInfo result = new DbSequenceInfo();
                result.setIncrementBy(rs.getLong("INCREMENT_BY"));
                result.setMinValue(rs.getLong("MIN_VALUE"));
                result.setMaxValue(rs.getLong("MAX_VALUE"));
                result.setCycle("Y".equals(rs.getString("CYCLE_FLAG")));
                return result;
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

    private String convertNameFromDb(String dbName, Grain g) {
        final String name;

        if (g.getScore().getIdentifierParser() instanceof AnsiQuotedIdentifierParser) {
            name = dbName.substring(g.getName().length() + 1);
        } else {
            Matcher m = TABLE_PATTERN.matcher(dbName);
            if (!m.find()) {
                return null;
            }
            name = m.group(2);
        }
        return name;
    }
}
