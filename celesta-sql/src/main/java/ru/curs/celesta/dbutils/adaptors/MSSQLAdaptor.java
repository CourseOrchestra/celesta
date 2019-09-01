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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;

import static ru.curs.celesta.dbutils.adaptors.constants.MsSqlConstants.*;

import ru.curs.celesta.dbutils.adaptors.ddl.*;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

/**
 * MSSQL Adaptor.
 */
public final class MSSQLAdaptor extends DBAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MSSQLAdaptor.class);

    private static final String SELECT_TOP_1 = "select top 1 %s from ";
    private static final String WHERE_S = " where %s;";

    public MSSQLAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new MsSqlDdlGenerator(this);
    }

    @Override
    public boolean tableExists(Connection conn, String schema, String name) {
        //TODO: It's a not good idea. We must check more concretely, cuz
        //      this method will work for other objects such as view etc.
        return objectExists(conn, schema, name);
    }

    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        try (PreparedStatement check = conn.prepareStatement("select count(*) from sys.tables;");
             ResultSet rs = check.executeQuery()) {
            rs.next();
            return rs.getInt(1) != 0;
        }
    }

    @Override
    void createSchemaIfNotExists(Connection conn, String name) {
        String sql = String.format(
                "select coalesce(SCHEMA_ID('%s'), -1)", name.replace("\"", "")
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();
            if (rs.getInt(1) == -1) {
                ddlAdaptor.createSchema(conn, name);
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public PreparedStatement getOneFieldStatement(Connection conn, Column<?> c, String where) {
        TableElement t = c.getParentTable();
        String sql = String.format(SELECT_TOP_1 + tableString(t.getGrain().getName(), t.getName())
                + WHERE_S, c.getQuotedName(), where);
        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement getOneRecordStatement(
            Connection conn, TableElement t, String where, Set<String> fields
    ) {

        final String filedList = getTableFieldsListExceptBlobs((DataGrainElement) t, fields);

        String sql = String.format(SELECT_TOP_1 + tableString(t.getGrain().getName(), t.getName()) + WHERE_S,
                filedList, where);
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
            sql = "insert into " + tableString(t.getGrain().getName(), t.getName()) + " default values;";
        } else {
            sql = String.format(
                    "insert " + tableString(t.getGrain().getName(), t.getName())
                            + " (%s) values (%s);", fields.toString(), params.toString()
            );
        }

        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) {
        String sql = String.format("delete " + tableString(t.getGrain().getName(), t.getName()) + WHERE_S, where);
        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) {
        // Готовим запрос на удаление
        String sql = String.format("delete " + tableString(t.getGrain().getName(), t.getName()) + " %s;",
                where.isEmpty() ? "" : "where " + where);
        try {
            PreparedStatement result = conn.prepareStatement(sql);
            return result;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public int getCurrentIdent(Connection conn, BasicTable t) {
        final String sql;

        IntegerColumn idColumn = t.getPrimaryKey().values().stream()
                .filter(c -> c instanceof IntegerColumn)
                .map(c -> (IntegerColumn) c)
                .filter(ic -> ic.getSequence() != null)
                .findFirst().get();


        sql = String.format(
                "SELECT CURRENT_VALUE FROM SYS.sequences WHERE name = '%s'",
                idColumn.getSequence().getName());

        try (Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(sql);
            if (!rs.next()) {
                throw new CelestaException("Id sequence for %s.%s is not initialized.", t.getGrain().getName(),
                        t.getName());
            }

            return (int) rs.getLong(1);

        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields,
                                    List<String> otherFields, String otherWhere) {
        String template = "EXISTS (SELECT * FROM %s WHERE %s AND %s)";

        String tableStr = tableString(dge.getGrain().getName(), dge.getName());
        String otherTableStr = tableString(otherDge.getGrain().getName(), otherDge.getName());

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < fields.size(); ++i) {
            sb.append(tableStr).append(".\"").append(fields.get(i)).append("\"")
                    .append(" = ")
                    .append(otherTableStr).append(".\"").append(otherFields.get(i)).append("\"");

            if (i + 1 != fields.size()) {
                sb.append(" AND ");
            }
        }

        String result = String.format(template, otherTableStr, sb.toString(), otherWhere);
        return result;
    }

    private boolean checkIfVarcharMax(Connection conn, Column<?> c) throws SQLException {
        PreparedStatement checkForMax = conn.prepareStatement(String.format(
                "select max_length from sys.columns where " + "object_id  = OBJECT_ID('%s.%s') and name = '%s'",
                c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName()));
        try {
            ResultSet rs = checkForMax.executeQuery();
            if (rs.next()) {
                int len = rs.getInt(1);
                return len == -1;
            }
        } finally {
            checkForMax.close();
        }
        return false;

    }

    /**
     * Returns information on column.
     *
     * @param conn DB connection
     * @param c    column
     */
    // CHECKSTYLE:OFF
    @Override
    public DbColumnInfo getColumnInfo(Connection conn, Column<?> c) {
        // CHECKSTYLE:ON
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(null, c.getParentTable().getGrain().getName(),
                    c.getParentTable().getName(), c.getName());
            try {
                if (rs.next()) {
                    DbColumnInfo result = new DbColumnInfo();
                    result.setName(rs.getString(COLUMN_NAME));
                    String typeName = rs.getString("TYPE_NAME");
                    if ("varbinary".equalsIgnoreCase(typeName) && checkIfVarcharMax(conn, c)) {
                        result.setType(BinaryColumn.class);
                    } else if ("int".equalsIgnoreCase(typeName)) {
                        result.setType(IntegerColumn.class);
                    } else if ("float".equalsIgnoreCase(typeName) && rs.getInt("COLUMN_SIZE") == DOUBLE_PRECISION) {
                        result.setType(FloatingColumn.class);
                    } else {
                        for (Class<? extends Column<?>> cc : COLUMN_CLASSES) {
                            if (getColumnDefiner(cc).dbFieldType().equalsIgnoreCase(typeName)) {
                                result.setType(cc);
                                break;
                            }
                        }
                    }
                    result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                    if (result.getType() == StringColumn.class) {
                        result.setLength(rs.getInt("COLUMN_SIZE"));
                        result.setMax(checkIfVarcharMax(conn, c));
                    }
                    if (result.getType() == DecimalColumn.class) {
                        result.setLength(rs.getInt("COLUMN_SIZE"));
                        result.setScale(rs.getInt("DECIMAL_DIGITS"));
                    }
                    String defaultBody = rs.getString("COLUMN_DEF");
                    if (defaultBody != null) {
                        int i = 0;
                        // Снимаем наружные скобки
                        while (defaultBody.charAt(i) == '('
                                && defaultBody.charAt(defaultBody.length() - i - 1) == ')') {
                            i++;
                        }
                        defaultBody = defaultBody.substring(i, defaultBody.length() - i);
                        if (IntegerColumn.class == result.getType()) {
                            Pattern p = Pattern.compile("NEXT VALUE FOR \\[.*]\\.\\[(.*)]");
                            Matcher m = p.matcher(defaultBody);
                            if (m.matches()) {
                                String sequenceName = m.group(1);
                                defaultBody = "NEXTVAL(" + sequenceName + ")";
                            }
                        }
                        if (BooleanColumn.class == result.getType()
                                || DateTimeColumn.class == result.getType()
                                || ZonedDateTimeColumn.class == result.getType()) {
                            defaultBody = defaultBody.toUpperCase();
                        }
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

    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {

        DbPkInfo result = new DbPkInfo(this);
        try {
            String sql = String.format(
                    "select cons.CONSTRAINT_NAME, cols.COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE cols "
                            + "inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS cons "
                            + "on cols.TABLE_SCHEMA = cons.TABLE_SCHEMA " + "and cols.TABLE_NAME = cons.TABLE_NAME "
                            + "and cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME "
                            + "where cons.CONSTRAINT_TYPE = 'PRIMARY KEY' and cons.TABLE_SCHEMA = '%s' "
                            + "and cons.TABLE_NAME = '%s' order by ORDINAL_POSITION",
                    t.getGrain().getName(), t.getName());
            LOGGER.trace(sql);
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
        // Full foreign key information query
        String sql = String.format(
                "SELECT RC.CONSTRAINT_SCHEMA AS 'GRAIN'" + "   , KCU1.CONSTRAINT_NAME AS 'FK_CONSTRAINT_NAME'"
                        + "   , KCU1.TABLE_NAME AS 'FK_TABLE_NAME'" + "   , KCU1.COLUMN_NAME AS 'FK_COLUMN_NAME'"
                        + "   , KCU2.TABLE_SCHEMA AS 'REF_GRAIN'" + "   , KCU2.TABLE_NAME AS 'REF_TABLE_NAME'"
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
        try {
            Statement stmt = conn.createStatement();
            try {
                DbFkInfo i = null;
                ResultSet rs = stmt.executeQuery(sql);
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
            } finally {
                stmt.close();
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
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
        String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;
        String rowFilter;

        final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);

        if (offset == 0) {
            // Запрос только с ограничением числа записей -- применяем MS SQL
            // Server TOP-конструкцию.
            sql = String.format("select top %d %s from %s", rowCount, fieldList,
                    from.getExpression()) + sqlwhere + " order by " + orderBy;
            return sql;
            // Иначе -- запрос с пропуском начальных записей -- применяем
            // ROW_NUMBER
        } else if (rowCount == 0) {
            rowFilter = String.format(">= %d", offset + 1L);
        } else {
            rowFilter = String.format("between %d and %d", offset + 1L, offset + rowCount);
        }
        sql = getLimitedSqlWithOffset(orderBy, fieldList, from.getExpression(), sqlwhere, rowFilter);
        return sql;
    }

    private String getLimitedSqlWithOffset(
            String orderBy, String fieldList, String from, String where, String rowFilter) {
        return String.format(
                "with a as " + "(select ROW_NUMBER() OVER (ORDER BY %s) as [limit_row_number], %s from %s %s) "
                        + " select * from a where [limit_row_number] %s",
                orderBy, fieldList, from, where, rowFilter);
    }

    @Override
    public Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) {
        String sql = String.format("select " + "    s.name as SchemaName," + "    o.name as TableName,"
                + "    i.name as IndexName," + "    co.name as ColumnName," + "    ic.key_ordinal as ColumnOrder "
                + "from sys.indexes i " + "inner join sys.objects o on i.object_id = o.object_id "
                + "inner join sys.index_columns ic on ic.object_id = i.object_id " + "    and ic.index_id = i.index_id "
                + "inner join sys.columns co on co.object_id = i.object_id " + "    and co.column_id = ic.column_id "
                + "inner join sys.schemas s on o.schema_id = s.schema_id "
                + "where i.is_primary_key = 0 and o.[type] = 'U' " + " and s.name = '%s' "
                + " order by o.name,  i.[name], ic.key_ordinal;", g.getName());

        LOGGER.trace(sql);

        Map<String, DbIndexInfo> result = new HashMap<>();
        try {
            Statement stmt = conn.createStatement();
            try {
                DbIndexInfo i = null;
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String tabName = rs.getString("TableName");
                    String indName = rs.getString("IndexName");
                    if (i == null || !i.getTableName().equals(tabName) || !i.getIndexName().equals(indName)) {
                        i = new DbIndexInfo(tabName, indName);
                        result.put(indName, i);
                    }
                    i.getColumnNames().add(rs.getString("ColumnName"));
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
    public boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException {
        String sql = String.format(
                "SELECT COUNT(*) FROM sys.triggers tr " + "INNER JOIN sys.tables t ON tr.parent_id = t.object_id "
                        + "WHERE t.schema_id = SCHEMA_ID('%s') and tr.name = '%s'",
                query.getSchema().replace("\"", ""),
                query.getName().replace("\"", "")
        );

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


    private String generateTsqlForVersioningTrigger(TableElement t) {
        StringBuilder sb = new StringBuilder();
        sb.append("IF  exists (select * from inserted inner join deleted on \n");
        addPKJoin(sb, "inserted", "deleted", t);
        sb.append("where inserted.recversion <> deleted.recversion) BEGIN\n");
        sb.append("  RAISERROR ('record version check failure', 16, 1);\n");

        sb.append("END\n");
        sb.append(String.format("update \"%s\".\"%s\" set recversion = recversion + 1 where%n",
                t.getGrain().getName(), t.getName()));
        sb.append("exists (select * from inserted where \n");

        addPKJoin(sb, "inserted", String.format("\"%s\".\"%s\"", t.getGrain().getName(), t.getName()),
                t);
        sb.append(");\n");

        return sb.toString();
    }

    //TODO:Must be defined in single place
    private void addPKJoin(StringBuilder sb, String left, String right, TableElement t) {
        boolean needAnd = false;
        for (String s : t.getPrimaryKey().keySet()) {
            if (needAnd) {
                sb.append(" AND ");
            }
            sb.append(String.format("  %s.\"%s\" = %s.\"%s\"%n", left, s, right, s));
            needAnd = true;
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
        boolean useWhere = w.length() > 0;


        final String sql;

        if (offset == 0) {
            if (orderBy.length() > 0) {
                w.append(" order by " + orderBy);
            }

            sql = String.format(SELECT_TOP_1 + " %s %s;", fieldList,
                    from.getExpression(), useWhere ? " where " + w : w);
        } else {
            sql = getLimitedSqlWithOffset(
                    orderBy, fieldList, from.getExpression(), useWhere ? " where " + w : w.toString(), "=" + offset);
        }

        LOGGER.trace(sql);
        return prepareStatement(conn, sql);
    }

    @Override
    public int getDBPid(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT @@SPID;")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            // do nothing
        }
        return 0;
    }

    @Override
    public boolean nullsFirst() {
        return true;
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        String sql = String.format("SELECT routine_name FROM INFORMATION_SCHEMA.ROUTINES "
                                 + "WHERE routine_schema = '%s' AND routine_type='FUNCTION'",
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
    String getSelectTriggerBodySql(TriggerQuery query) {
        String sql = String.format(" SELECT OBJECT_DEFINITION (id)%n"
                                 + "        FROM sysobjects%n"
                                 + "    WHERE id IN(SELECT tr.object_id%n"
                                 + "        FROM sys.triggers tr%n"
                                 + "        INNER JOIN sys.tables t ON tr.parent_id = t.object_id%n"
                                 + "        WHERE t.schema_id = SCHEMA_ID('%s')%n"
                                 + "        AND tr.name = '%s');",
                query.getSchema(), query.getName());

        return sql;
    }

    @Override
    public DBType getType() {
        return DBType.MSSQL;
    }

    @Override
    public long nextSequenceValue(Connection conn, SequenceElement s) {
        String sql = "SELECT NEXT VALUE FOR " + sequenceString(s.getGrain().getName(), s.getName());

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new CelestaException(
                    "Can't get next value of sequence " + tableString(s.getGrain().getName(), s.getName()), e
            );
        }
    }

    @Override
    public boolean sequenceExists(Connection conn, String schema, String name) {
        //TODO: It's a not good idea. We must check more concretely, cuz
        //      this method will work for other objects such as view etc.
        return objectExists(conn, schema, name);
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        String sql = "SELECT CAST(INCREMENT AS varchar(max)) AS INCREMENT,"
                         + " CAST(MINIMUM_VALUE AS varchar(max)) AS MINIMUM_VALUE,"
                         + " CAST(MAXIMUM_VALUE AS varchar(max)) AS MAXIMUM_VALUE,"
                         + " CAST(IS_CYCLING AS varchar(max)) AS IS_CYCLING"
                  + " FROM SYS.SEQUENCES WHERE SCHEMA_ID = SCHEMA_ID (?) AND NAME = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, s.getGrain().getName());
            preparedStatement.setString(2, s.getName());
            try (ResultSet rs = preparedStatement.executeQuery()) {
                rs.next();
                DbSequenceInfo result = new DbSequenceInfo();
                result.setIncrementBy(rs.getLong("INCREMENT"));
                result.setMinValue(rs.getLong("MINIMUM_VALUE"));
                result.setMaxValue(rs.getLong("MAXIMUM_VALUE"));
                result.setCycle(rs.getBoolean("IS_CYCLING"));
                return result;
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

    private boolean objectExists(Connection conn, String schema, String name) {
        String sql = String.format(
                "select coalesce(object_id('%s.%s'), -1)",
                schema.replace("\"", ""),
                name.replace("\"", "")
        );
        try (Statement check = conn.createStatement();
             ResultSet rs = check.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) != -1;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }
}
