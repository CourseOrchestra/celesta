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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;

import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlGenerator;
import ru.curs.celesta.dbutils.adaptors.ddl.PostgresDdlGenerator;

import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.dbutils.meta.DbFkInfo;
import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.dbutils.meta.DbPkInfo;
import ru.curs.celesta.dbutils.meta.DbSequenceInfo;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.DecimalColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.TableElement;

import static ru.curs.celesta.dbutils.adaptors.constants.OpenSourceConstants.CONJUGATE_INDEX_POSTFIX;
import static ru.curs.celesta.dbutils.adaptors.constants.OpenSourceConstants.NOW;


/**
 * Postgres adaptor.
 */
final public class PostgresAdaptor extends OpenSourceDbAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresAdaptor.class);

    private static final Pattern HEX_STRING = Pattern.compile("'\\\\x([0-9A-Fa-f]+)'");

    public PostgresAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new PostgresDdlGenerator(this);
    }

    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        try (PreparedStatement check = conn.prepareStatement("select count(*) from information_schema.tables "
                + "where table_type = 'BASE TABLE' " + "and table_schema not in ('pg_catalog', 'information_schema');");
             ResultSet rs = check.executeQuery()) {
            rs.next();
            return rs.getInt(1) != 0;
        }
    }

    @Override
    public int getCurrentIdent(Connection conn, BasicTable t) {

        IntegerColumn idColumn = t.getPrimaryKey().values().stream()
                .filter(c -> c instanceof IntegerColumn)
                .map(c -> (IntegerColumn) c)
                .filter(ic -> ic.getSequence() != null)
                .findFirst().get();

        String sequenceName = idColumn.getSequence().getName();

        String sql = String.format("select last_value from \"%s\".\"%s\"", t.getGrain().getName(), sequenceName);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
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

        String returning = "";
        for (Column<?> c : t.getColumns().values()) {
            if (c instanceof IntegerColumn) {
                IntegerColumn ic = (IntegerColumn) c;

                if (ic.getSequence() != null) {
                    returning = " returning " + c.getQuotedName();
                    break;
                }
            }
        }

        final String sql;

        if (fields.length() == 0 && params.length() == 0) {
            sql = String.format("insert into " + tableString(t.getGrain().getName(),
                    t.getName()) + " default values %s;", returning);
        } else {
            sql = String.format("insert into " + tableString(t.getGrain().getName(),
                    t.getName()) + " (%s) values (%s)%s;", fields.toString(), params.toString(), returning);
        }

        return prepareStatement(conn, sql);
    }

    @Override
    public DbColumnInfo getColumnInfo(Connection conn, Column<?> c) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(null, c.getParentTable().getGrain().getName()
                            .replace("\"", ""),
                    c.getParentTable().getName().replace("\"", ""), c.getName()
                            .replace("\"", ""))) {
                if (rs.next()) {
                    DbColumnInfo result = new DbColumnInfo();
                    result.setName(rs.getString(COLUMN_NAME));
                    String typeName = rs.getString("TYPE_NAME");
                    if ("serial".equalsIgnoreCase(typeName)) {
                        result.setType(IntegerColumn.class);
                        result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);

                        String defaultBody = rs.getString("COLUMN_DEF");
                        Pattern p = Pattern.compile("nextval\\('[\"]?[^\"]+[\"]?\\.[\"]?([^\"]+)+[\"]?'::regclass\\)");
                        Matcher m = p.matcher(defaultBody);

                        if (m.matches()) {
                            String sequenceName = m.group(1);
                            result.setDefaultValue("NEXTVAL(" + sequenceName + ")");
                        }

                        return result;
                    } else if ("text".equalsIgnoreCase(typeName)) {
                        result.setType(StringColumn.class);
                        result.setMax(true);
                    } else {
                        for (Class<? extends Column<?>> cc : COLUMN_CLASSES) {
                            if (getColumnDefiner(cc).dbFieldType().equalsIgnoreCase(typeName)) {
                                result.setType(cc);
                                break;
                            }
                        }
                    }
                    result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                    if (result.getType() == StringColumn.class || result.getType() == DecimalColumn.class) {
                        result.setLength(rs.getInt("COLUMN_SIZE"));
                    }
                    if (result.getType() == DecimalColumn.class) {
                        result.setScale(rs.getInt("DECIMAL_DIGITS"));
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
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    private String modifyDefault(DbColumnInfo ci, String defaultBody) {
        String result = defaultBody;
        if (DateTimeColumn.class == ci.getType()) {
            if (NOW.equalsIgnoreCase(defaultBody)) {
                result = "GETDATE()";
            } else {
                Matcher m = DATEPATTERN.matcher(defaultBody);
                m.find();
                result = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
            }
        } else if (BooleanColumn.class == ci.getType()) {
            result = "'" + defaultBody.toUpperCase() + "'";
        } else if (StringColumn.class == ci.getType()) {
            if (result.endsWith("::text")) {
                result = result.substring(0, result.length() - "::text".length());
            } else if (result.endsWith("::character varying")) {
                result = result.substring(0, result.length() - "::character varying".length());
            }
        } else if (BinaryColumn.class == ci.getType()) {
            Matcher m = HEX_STRING.matcher(defaultBody);
            if (m.find()) {
                result = "0x" + m.group(1).toUpperCase();
            }
        }
        return result;
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

    @Override
    String getLimitedSQL(
            FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields
    ) {
        if (offset == 0 && rowCount == 0) {
            throw new IllegalArgumentException();
        }
        String sql;
        if (offset == 0) {
            sql = getSelectFromOrderBy(from, whereClause, orderBy, fields)
                    + String.format(" limit %d", rowCount);
        } else if (rowCount == 0) {
            sql = getSelectFromOrderBy(from, whereClause, orderBy, fields)
                    + String.format(" limit all offset %d", offset);
        } else {
            sql = getSelectFromOrderBy(from, whereClause, orderBy, fields)
                    + String.format(" limit %d offset %d", rowCount, offset);
        }
        return sql;
    }


    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        String sql = String.format(
                " SELECT r.routine_name FROM INFORMATION_SCHEMA.ROUTINES r "
               + "WHERE r.routine_schema = '%s' AND r.routine_type='FUNCTION' "
                   + "AND exists (select * from pg_proc p\n"
                       + "        where p.proname = r.routine_name\n"
                           + "        AND upper(pg_get_function_result(p.oid)) like upper('%%table%%'))",
                g.getName());
        List<String> result = new LinkedList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new CelestaException("Cannot get parameterized views list: %s", e.toString());
        }
        return result;
    }

    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        String sql = String.format(
                "SELECT i.relname AS indexname, " + "i.oid, array_length(x.indkey, 1) as colcount " + "FROM pg_index x "
                        + "INNER JOIN pg_class c ON c.oid = x.indrelid "
                        + "INNER JOIN pg_class i ON i.oid = x.indexrelid "
                        + "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
                        + "WHERE c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" "
                        + "and n.nspname = '%s' and c.relname = '%s' and x.indisprimary",
                t.getGrain().getName().replace("\"", ""), t.getName().replace("\"", ""));
        DbPkInfo result = new DbPkInfo(this);

        try (Statement stmt = conn.createStatement();
             PreparedStatement stmt2 = conn.prepareStatement("select pg_get_indexdef(?, ?, false)");
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String indName = rs.getString("indexname");
                int colCount = rs.getInt("colcount");
                int oid = rs.getInt("oid");
                result.setName(indName);
                stmt2.setInt(1, oid);
                for (int i = 1; i <= colCount; i++) {
                    stmt2.setInt(2, i);
                    try (ResultSet rs2 = stmt2.executeQuery()) {
                        rs2.next();
                        String colName = rs2.getString(1);
                        Matcher m = QUOTED_NAME.matcher(colName);
                        m.matches();
                        result.addColumnName(m.group(1));
                    }
                }
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not get indices information: %s", e.getMessage());
        }
        return result;
    }

    @Override
    public List<DbFkInfo> getFKInfo(Connection conn, Grain g) {
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

        LOGGER.trace(sql);

        List<DbFkInfo> result = new LinkedList<>();
        try (Statement stmt = conn.createStatement()) {
            DbFkInfo i = null;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String fkName = rs.getString("FK_CONSTRAINT_NAME");
                    if (i == null || !i.getName().equals(fkName)) {
                        i = new DbFkInfo(fkName);
                        result.add(i);
                        i.setTableName(rs.getString("FK_TABLE_NAME"));
                        i.setRefGrainName(rs.getString("REF_GRAIN"));
                        i.setRefTableName(rs.getString("REF_TABLE_NAME"));
                        i.setUpdateRule(getFKRule(rs.getString("UPDATE_RULE")));
                        i.setDeleteRule(getFKRule(rs.getString("DELETE_RULE")));
                    }
                    i.getColumnNames().add(rs.getString("FK_COLUMN_NAME"));
                }
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
        return result;
    }

    @Override
    public Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) {
        String sql = String.format("SELECT c.relname AS tablename, i.relname AS indexname, "
                + "i.oid, array_length(x.indkey, 1) as colcount " + "FROM pg_index x "
                + "INNER JOIN pg_class c ON c.oid = x.indrelid " + "INNER JOIN pg_class i ON i.oid = x.indexrelid "
                + "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "WHERE c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" "
                + "and n.nspname = '%s' and x.indisunique = false;", g.getName());
        Map<String, DbIndexInfo> result = new HashMap<>();
        try {
            Statement stmt = conn.createStatement();
            PreparedStatement stmt2 = conn.prepareStatement("select pg_get_indexdef(?, ?, false)");
            try {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String tabName = rs.getString("tablename");
                    String indName = rs.getString("indexname");
                    if (indName.endsWith(CONJUGATE_INDEX_POSTFIX)) {
                        continue;
                    }
                    DbIndexInfo ii = new DbIndexInfo(tabName, indName);
                    result.put(indName, ii);
                    int colCount = rs.getInt("colcount");
                    int oid = rs.getInt("oid");
                    stmt2.setInt(1, oid);
                    for (int i = 1; i <= colCount; i++) {
                        stmt2.setInt(2, i);
                        ResultSet rs2 = stmt2.executeQuery();
                        try {
                            rs2.next();
                            String colName = rs2.getString(1);
                            Matcher m = QUOTED_NAME.matcher(colName);
                            m.matches();
                            ii.getColumnNames().add(m.group(1));
                        } finally {
                            rs2.close();
                        }
                    }

                }
            } finally {
                stmt.close();
                stmt2.close();
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not get indices information: %s", e.getMessage());
        }
        return result;
    }


    @Override
    public void createSysObjects(Connection conn, String sysSchemaName) {
        String sql = "CREATE OR REPLACE FUNCTION " + sysSchemaName + ".recversion_check()"
                + "  RETURNS trigger AS $BODY$ BEGIN\n"
                + "    IF (OLD.recversion = NEW.recversion) THEN\n"
                + "       NEW.recversion = NEW.recversion + 1;\n     ELSE\n"
                + "       RAISE EXCEPTION 'record version check failure';\n" + "    END IF;"
                + "    RETURN NEW; END; $BODY$\n" + "  LANGUAGE plpgsql VOLATILE COST 100;";

        try {
            Statement stmt = conn.createStatement();
            try {
                stmt.executeUpdate(sql);
            } finally {
                stmt.close();
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not create or replace " + sysSchemaName
                    + ".recversion_check() function: %s", e.getMessage());
        }
    }


    @Override
    public boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException {
        String sql = String.format("select count(*) from information_schema.triggers where "
                        + "        event_object_schema = '%s' and event_object_table= '%s'"
                        + "        and trigger_name = '%s'",
                query.getSchema().replace("\"", ""),
                query.getTableName().replace("\"", ""),
                query.getName());

        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            boolean result = rs.getInt(1) > 0;
            rs.close();
            return result;
        } finally {
            stmt.close();
        }

    }

    @Override
    public int getDBPid(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select pg_backend_pid();")) {
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
        ZoneOffset systemOffset = OffsetDateTime.now().getOffset();
        ZoneOffset offset = z.getOffset();
        int offsetDifInSeconds = systemOffset.getTotalSeconds() - offset.getTotalSeconds();
        return z.plusSeconds(offsetDifInSeconds);
    }

    @Override
    String getSelectTriggerBodySql(TriggerQuery query) {
        String sql = String.format(
                "select DISTINCT(prosrc)\n"
             + " from pg_trigger, pg_proc, information_schema.triggers\n"
             + " where\n"
                 + " pg_proc.oid=pg_trigger.tgfoid\n"
                 + " and information_schema.triggers.trigger_schema='%s'\n"
                 + " and information_schema.triggers.event_object_table='%s'"
                 + " and pg_trigger.tgname = '%s'\n",
                query.getSchema(), query.getTableName(), query.getName());

        return sql;
    }

    @Override
    public DBType getType() {
        return DBType.POSTGRESQL;
    }

    @Override
    public boolean supportsCortegeComparing() {
        return true;
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        String sql = "SELECT INCREMENT, MINIMUM_VALUE, MAXIMUM_VALUE, CYCLE_OPTION"
                  + " FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ? AND SEQUENCE_NAME = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, s.getGrain().getName().replace("\"", ""));
            preparedStatement.setString(2, s.getName().replace("\"", ""));
            try (ResultSet rs = preparedStatement.executeQuery()) {
                rs.next();
                DbSequenceInfo result = new DbSequenceInfo();
                result.setIncrementBy(rs.getLong("INCREMENT"));
                result.setMinValue(rs.getLong("MINIMUM_VALUE"));
                result.setMaxValue(rs.getLong("MAXIMUM_VALUE"));
                result.setCycle(rs.getBoolean("CYCLE_OPTION"));
                return result;
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }
}
