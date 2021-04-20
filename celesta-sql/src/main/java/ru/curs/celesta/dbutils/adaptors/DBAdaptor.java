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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefiner;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlAdaptor;

import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlGenerator;
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
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.DecimalColumn;
import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.TableElement;
import ru.curs.celesta.score.VersionedElement;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ZonedDateTimeColumn;

import static ru.curs.celesta.dbutils.adaptors.function.CommonFunctions.getFieldList;
import static ru.curs.celesta.dbutils.adaptors.function.CommonFunctions.padComma;
import static ru.curs.celesta.dbutils.jdbc.SqlUtils.executeQuery;
import static ru.curs.celesta.dbutils.jdbc.SqlUtils.executeUpdate;

/**
 * Adaptor for connection to the database.
 */
public abstract class DBAdaptor implements QueryBuildingHelper, StaticDataAdaptor {

    /*
     * N.B. for contributors. This class is great, so To avoid chaos,
     * here is the order of (except constructors and fabric methods):
     * first of all -- public final methods,
     * then -- package-private static methods,
     * then -- package-private final methods,
     * then -- package-private methods,
     * then -- package-private abstract methods,
     * then -- public static methods,
     * then -- public final methods,
     * then -- public methods,
     * then -- public abstract methods,
     * then -- private methods
     */

    static final List<Class<? extends Column<?>>> COLUMN_CLASSES = Arrays.asList(
            IntegerColumn.class,
            StringColumn.class,
            BooleanColumn.class,
            FloatingColumn.class,
            DecimalColumn.class,
            BinaryColumn.class,
            DateTimeColumn.class,
            ZonedDateTimeColumn.class);

    static final String COLUMN_NAME = "COLUMN_NAME";

    private static final Logger LOGGER = LoggerFactory.getLogger(DBAdaptor.class);

    protected final ConnectionPool connectionPool;
    DdlAdaptor ddlAdaptor;

    //TODO: Javadoc
    protected DBAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        this.connectionPool = connectionPool;
        this.ddlAdaptor = new DdlAdaptor(getDdlGenerator(), ddlConsumer);
        connectionPool.setDbAdaptor(this);
    }

    abstract DdlGenerator getDdlGenerator();

    // =========> PACKAGE-PRIVATE STATIC METHODS <=========

    /**
     * Creates a PreparedStatement object.
     *
     * @param conn Connection to use.
     * @param sql  SQL statement.
     * @return new default PreparedStatement object.
     */
    static PreparedStatement prepareStatement(Connection conn, String sql) {
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    /**
     * Transforms {@link Iterable<String>} of field names into comma separated {@link String} field names.
     * Binary fields are excluded from result.
     *
     * @param t      the {@link DataGrainElement} type, that's owner of fields.
     * @param fields {@link Iterable<String>} fields to transform.
     * @return Comma separated {@link String} field names.
     */
    static String getTableFieldsListExceptBlobs(DataGrainElement t, Set<String> fields) {
        final List<String> flds;

        Predicate<ColumnMeta<?>> notBinary = c -> !BinaryColumn.CELESTA_TYPE.equals(c.getCelestaType());

        if (fields.isEmpty()) {
            flds = t.getColumns().entrySet().stream()
                    .filter(e -> notBinary.test(e.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        } else {
            flds = fields.stream()
                    .filter(f -> notBinary.test(t.getColumns().get(f)))
                    .collect(Collectors.toList());
        }
        // To the list of fields of the versioned tables we necessarily add "recversion"
        if (t instanceof Table && ((Table) t).isVersioned()) {
            flds.add(VersionedElement.REC_VERSION);
        }

        return getFieldList(flds);
    }

    /**
     * Returns {@link FKRule} by input string rule.
     * The method is case-insensitive for rule param.
     *
     * @param rule input string.
     * @return Returns one of the values of {@link FKRule} or null in case of invalid input.
     */
    static FKRule getFKRule(String rule) {
        if ("NO ACTION".equalsIgnoreCase(rule) || "RECTRICT".equalsIgnoreCase(rule)) {
            return FKRule.NO_ACTION;
        }
        if ("SET NULL".equalsIgnoreCase(rule)) {
            return FKRule.SET_NULL;
        }
        if ("CASCADE".equalsIgnoreCase(rule)) {
            return FKRule.CASCADE;
        }
        return null;
    }


    /**
     * Executes sql query and then adds a column values with index 1 to {@link Set<String>} to return.
     *
     * @param conn Connection to use.
     * @param sql  Sql query to execute.
     * @return {@link Set<String>} with values of column with index 1,
     * which were received as a result of the sql query.
     */
    static Set<String> sqlToStringSet(Connection conn, String sql) {
        Set<String> result = new HashSet<>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            try {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
            } finally {
                stmt.close();
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
        return result;
    }

    // =========> END PACKAGE-PRIVATE STATIC METHODS <=========

    // =========> PACKAGE-PRIVATE FINAL METHODS <=========

    /**
     * Return String representation of sql query to select data with "ORDER BY" expression.
     *
     * @param from        FROM metadata.
     * @param whereClause WHERE clause to use in resulting query.
     * @param orderBy     ORDER BY clause to use in resulting query.
     * @param fields      fields for select by a resulting  query.
     * @return Return String representation of sql query to select data with "ORDER BY" expression.
     */
    final String getSelectFromOrderBy(
            FromClause from, String whereClause, String orderBy, Set<String> fields
    ) {
        final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);
        String sqlfrom = String.format("select %s from %s", fieldList,
                from.getExpression());

        String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

        return sqlfrom + sqlwhere + " order by " + orderBy;
    }
    // =========> END PACKAGE-PRIVATE FINAL METHODS <=========


    // =========> PACKAGE-PRIVATE METHODS <=========
    /**
     * Returns FROM clause for selection of a constant in SQL.
     * @return
     */
    String constantFromSql() {
        return "";
    }

    //TODO: Javadoc
    String prepareRowColumnForSelectStaticStrings(String value, String colName, int maxStringLength) {
        return "? as " + colName;
    }

    final ColumnDefiner getColumnDefiner(Class<? extends Column<?>> c) {
        return ColumnDefinerFactory.getColumnDefiner(getType(), c);
    }
    // =========> END PACKAGE-PRIVATE METHODS <=========


    // =========> PACKAGE-PRIVATE ABSTRACT METHODS <=========
    /**
     * Builds SELECT expression that selects restricted amount of records starting
     * from an offset.
     *
     * @param from
     * @param whereClause
     * @param orderBy
     * @param offset
     * @param rowCount
     * @param fields
     * @return
     */
    abstract String getLimitedSQL(
            FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields
    );

    //TODO: Javadoc
    abstract String getSelectTriggerBodySql(TriggerQuery query);

    /**
     * Whether user defined tables exist in the DB.
     *
     * @param conn  DB connection
     * @return
     * @throws SQLException
     */
    abstract boolean userTablesExist(Connection conn) throws SQLException;

    /**
     * Creates DB schema if it is absent.
     *
     * @param conn  DB connection
     * @param name  schema name
     */
    abstract void createSchemaIfNotExists(Connection conn, String name);
    // =========> END PACKAGE-PRIVATE ABSTRACT METHODS <=========

    // =========> PUBLIC STATIC METHODS <=========
    // =========> END PUBLIC STATIC METHODS <=========


    // =========> PUBLIC FINAL METHODS <=========

    /**
     * Deletes table from RDBMS.
     *
     * @param conn Connection to use.
     * @param t    TableElement metadata of deleting table provided by Celesta.
     */
    public final void dropTable(Connection conn, TableElement t) {
        this.ddlAdaptor.dropTable(conn, t);
    }

    /**
     * Returns {@code true} in that and only that case if DB contains user tables
     * (i.e. DB is not empty).
     */
    public final boolean userTablesExist() {
        try (Connection conn = connectionPool.get()) {
            return userTablesExist(conn);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    /**
     * Creates DB schema with the specified name if such didn't exist before.
     *
     * @param name  schema name.
     */
    public final void createSchemaIfNotExists(String name) {
        try (Connection conn = connectionPool.get()) {
            createSchemaIfNotExists(conn, name);
            conn.commit();
        } catch (SQLException e) {
            throw new CelestaException("Cannot create schema. " + e.getMessage());
        }
    }

    /**
     * Adds a new column to the table.
     *
     * @param conn  DB connection
     * @param c  column
     */
    public final void createColumn(Connection conn, Column<?> c) {
        this.ddlAdaptor.createColumn(conn, c);
    }

    // CHECKSTYLE:OFF 6 parameters
    /**
     * Builds prepared statement for records UPDATE.<br/>
     * <br/>
     * {@code equalsMask[columnIndex]} should contain {@code true} for the column with
     * index equal to {@code columnIndex} to take part in the evaluation.<br/>
     * If {@code nullsMask[columnIndex]} contains {@code true} {@code IS NULL} check
     * has a priority above {@code program[columnIndex]} check - {@code column = ?}.
     *
     * @param conn  DB connection
     * @param t  updatable table
     * @param equalsMask  equals mask
     * @param nullsMask  nulls mask
     * @param program  collects parameter predicates
     * @param where  WHERE clause
     * @return
     */
    public final PreparedStatement getUpdateRecordStatement(
            Connection conn, BasicTable t, boolean[] equalsMask,
            boolean[] nullsMask, List<ParameterSetter> program, String where) {

        // CHECKSTYLE:ON
        StringBuilder setClause = new StringBuilder();
        if (t instanceof Table && ((Table) t).isVersioned()) {
            setClause.append(String.format("\"%s\" = ?", VersionedElement.REC_VERSION));
            program.add(ParameterSetter.createForRecversion(this));
        }

        int i = 0;
        for (String c : t.getColumns().keySet()) {
            // Пропускаем ключевые поля и поля, не изменившие своего значения
            if (!(equalsMask[i] || t.getPrimaryKey().containsKey(c))) {
                padComma(setClause);
                if (nullsMask[i]) {
                    setClause.append(String.format("\"%s\" = NULL", c));
                } else {
                    setClause.append(String.format("\"%s\" = ?", c));
                    program.add(ParameterSetter.create(t.getColumnIndex(c), this));
                }
            }
            i++;
        }

        String sql = String.format("update " + tableString(t.getGrain().getName(), t.getName()) + " set %s where %s",
                setClause.toString(), where);

        LOGGER.trace(sql);
        return prepareStatement(conn, sql);
    }

    /**
     * Creates a table index in the DB.
     *
     * @param conn  DB connection
     * @param index  table index
     */
    public final void createIndex(Connection conn, Index index) {
        this.ddlAdaptor.createIndex(conn, index);
    }

    /**
     * Creates a foreign key in the DB.
     *
     * @param conn  DB connection
     * @param fk  foreign key
     */
    public final void createFK(Connection conn, ForeignKey fk) {
        this.ddlAdaptor.createFk(conn, fk);
    }

    /**
     * Removes table index in the grain.
     *
     * @param g           Grain
     * @param dBIndexInfo Information on index
     */
    public final void dropIndex(Grain g, DbIndexInfo dBIndexInfo) {
        try (Connection conn = connectionPool.get()) { //TODO: Why there is a new Connection instance
            ddlAdaptor.dropIndex(conn, g, dBIndexInfo);
            conn.commit();
        } catch (CelestaException | SQLException e) {
            throw new CelestaException("Cannot drop index '%s': %s ", dBIndexInfo.getIndexName(), e.getMessage());
        }
    }

    /**
     * Returns {@link PreparedStatement} containing a filtered set of entries.
     *
     * @param conn         Connection
     * @param from         Object for forming FROM part of the query
     * @param whereClause  Where clause
     * @param orderBy      Sort order
     * @param offset       Number of entries to skip
     * @param rowCount     Number of entries to return (limit filter)
     * @param fields       Requested columns. If none are provided all columns are requested
     */
    // CHECKSTYLE:OFF 7 parameters
    public final PreparedStatement getRecordSetStatement(
            Connection conn, FromClause from, String whereClause,
            String orderBy, long offset, long rowCount, Set<String> fields
    ) {
        // CHECKSTYLE:ON
        String sql;

        if (offset == 0 && rowCount == 0) {
            // The query is not limited -- it is same for all DBMS
            // Joining all received components into a standard query
            // SELECT..FROM..WHERE..ORDER BY
            sql = getSelectFromOrderBy(from, whereClause, orderBy, fields);
        } else {
            sql = getLimitedSQL(from, whereClause, orderBy, offset, rowCount, fields);

            LOGGER.trace(sql);
        }
        try {
            PreparedStatement result = conn.prepareStatement(sql);
            return result;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    /**
     * Builds a SELECT COUNT statement.
     *
     * @param conn  Connection
     * @param from  From clause
     * @param whereClause  Where clause
     * @return
     */
    public final PreparedStatement getSetCountStatement(Connection conn, FromClause from, String whereClause) {
        String sql = "select count(*) from " + from.getExpression()
                + ("".equals(whereClause) ? "" : " where " + whereClause);
        PreparedStatement result = prepareStatement(conn, sql);

        return result;
    }

    /**
     * Drops a trigger from DB.
     *
     * @param conn  Connection
     * @param query  Trigger query
     */
    public final void dropTrigger(Connection conn, TriggerQuery query) {
        ddlAdaptor.dropTrigger(conn, query);
    }

    public final void updateVersioningTrigger(Connection conn, TableElement t) {
        ddlAdaptor.updateVersioningTrigger(conn, t);
    }

    /**
     * Creates primary key on a table.
     *
     * @param conn  DB connection
     * @param t  table
     */
    public final void createPK(Connection conn, TableElement t) {
        this.ddlAdaptor.createPk(conn, t);
    }

    public final SQLGenerator getViewSQLGenerator() {
        return this.ddlAdaptor.getViewSQLGenerator();
    }

    /**
     * Creates a view in the DB based on metadata.
     *
     * @param conn DB connection
     * @param v    View
     */
    public final void createView(Connection conn, View v) {
        this.ddlAdaptor.createView(conn, v);
    }

    public final void createParameterizedView(Connection conn, ParameterizedView pv) {
        this.ddlAdaptor.createParameterizedView(conn, pv);
    }

    public final void dropTableTriggersForMaterializedViews(Connection conn, BasicTable t) {
        this.ddlAdaptor.dropTableTriggersForMaterializedViews(conn, t);
    }

    public final void createTableTriggersForMaterializedViews(Connection conn, BasicTable t) {
        this.ddlAdaptor.createTableTriggersForMaterializedViews(conn, t);
    }

    public final void executeNative(Connection conn, String sql) {
        this.ddlAdaptor.executeNative(conn, sql);
    }
    // =========> END PUBLIC FINAL METHODS <=========


    // =========> PUBLIC METHODS <=========

    /**
     * Checking for connection validity.
     *
     * @param conn    connection
     * @param timeout time-out
     * @return {@code true} if connection is valid, otherwise {@code false}
     */
    public boolean isValidConnection(Connection conn, int timeout) {
        try {
            return conn.isValid(timeout);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    /**
     * Returns template by table name.
     *
     * @param schemaName  schema name
     * @param tableName  table name
     */
    public String tableString(String schemaName, String tableName) {
        return getSchemaDotNameQuotedTemplate(schemaName, tableName);
    }

    private String getSchemaDotNameQuotedTemplate(String schemaName, String name) {
        StringBuilder sb = new StringBuilder();

        if (schemaName.startsWith("\"")) {
            sb.append(schemaName);
        } else {
            sb.append("\"").append(schemaName).append("\"");
        }

        sb.append(".");

        if (name.startsWith("\"")) {
            sb.append(name);
        } else {
            sb.append("\"").append(name).append("\"");
        }

        return sb.toString();
    }

    /**
     * Returns template by sequence name.
     *
     * @param schemaName  schema name
     * @param sequenceName  sequence name
     * @return
     */
    public String sequenceString(String schemaName, String sequenceName) {
        return getSchemaDotNameQuotedTemplate(schemaName, sequenceName);
    }


    /**
     * Returns DB specific PK constraint name for a table element.
     *
     * @param tableElement  table element
     */
    public String pkConstraintString(TableElement tableElement) {
        return tableElement.getPkConstraintName();
    }

    /**
     * Creates a table "from scratch" in the database.
     *
     * @param conn Connection
     * @param te   Table for creation.
     * tables also in case if such table exists.
     */
    public void createTable(Connection conn, TableElement te) {
        ddlAdaptor.createTable(conn, te);
    }

    /**
     * Returns a set of column names for a specific table.
     *
     * @param conn DB connection
     * @param t    Table to look the columns in.
     */
    public Set<String> getColumns(Connection conn, TableElement t) {
        Set<String> result = new LinkedHashSet<>();
        try (
            ResultSet rs = conn.getMetaData()
                .getColumns(null, t.getGrain().getName(), t.getName(), null)
        ) {
            while (rs.next()) {
                String rColumnName = rs.getString(COLUMN_NAME);
                result.add(rColumnName);
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
        }

        return result;
    }

    /**
     * Drops a foreign key from the database.
     *
     * @param conn       DB connection
     * @param schemaName schema name
     * @param tableName  table possessing the foreign key
     * @param fkName     name of foreign key
     */
    public void dropFK(Connection conn, String schemaName, String tableName, String fkName) {
        try {
            this.ddlAdaptor.dropFK(conn, schemaName, tableName, fkName);
        } catch (CelestaException e) {
            throw new CelestaException("Cannot drop foreign key '%s': %s", fkName, e.getMessage());
        }
    }

    /**
     * Drops parameterized view from the database.
     *
     * @param conn       DB connection
     * @param schemaName schema name
     * @param viewName   view name
     */
    public void dropParameterizedView(Connection conn, String schemaName, String viewName) {
        this.ddlAdaptor.dropParameterizedView(conn, schemaName, viewName);
    }

    /**
     * Returns list of view names in the grain.
     *
     * @param conn DB connection
     * @param g    Grain for which the list of view names have to be returned.
     */
    public List<String> getViewList(Connection conn, Grain g) {
        String sql = String.format("select table_name from information_schema.views where table_schema = '%s'",
                g.getName());
        List<String> result = new LinkedList<>();
        try (ResultSet rs = executeQuery(conn, sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (SQLException | CelestaException e) {
            throw new CelestaException("Cannot get views list: %s", e.toString());
        }
        return result;
    }

    //TODO: Javadoc
    public String getCallFunctionSql(ParameterizedView pv) {
        return String.format(
                tableString(pv.getGrain().getName(), pv.getName()) + "(%s)",
                pv.getParameters().keySet().stream()
                        .map(p -> "?")
                        .collect(Collectors.joining(", "))
        );
    }

    /**
     * Creates a sequence in the database.
     *
     * @param conn  DB connection
     * @param s  sequence element
     */
    public void createSequence(Connection conn, SequenceElement s) {
        ddlAdaptor.createSequence(conn, s);
    }

    /**
     * Alters sequence in the database.
     *
     * @param conn DB connection
     * @param s sequence element
     */
    public void alterSequence(Connection conn, SequenceElement s) {
        ddlAdaptor.alterSequence(conn, s);
    }

    /**
     * Drops sequence from the database.
     *
     * @param conn DB connection
     * @param s sequence element
     */
    public void dropSequence(Connection conn, SequenceElement s) {
        String sql = String.format("DROP SEQUENCE " + sequenceString(s.getGrain().getName(), s.getName()));
        executeUpdate(conn, sql);
    }

    /**
     * Drops view.
     *
     * @param conn       DB connection
     * @param schemaName grain name
     * @param viewName   view name
     */
    public void dropView(Connection conn, String schemaName, String viewName) {
        ddlAdaptor.dropView(conn, schemaName, viewName);
    }

    /**
     * Creates or recreates other system objects (stored procedures, functions)
     * needed for Celesta functioning on current RDBMS.
     *
     * @param conn  DB connection
     * @param sysSchemaName  system schema name
     */
    public void createSysObjects(Connection conn, String sysSchemaName) {

    }

    /**
     * Translates Celesta date literal to the one from specific database.
     *
     * @param date  Date literal
     * @return
     */
    public String translateDate(String date) {
        try {
            DateTimeColumn.parseISODate(date);
        } catch (ParseException e) {
            throw new CelestaException(e.getMessage());
        }
        return date;
    }

    //TODO: Javadoc
    public Optional<String> getTriggerBody(Connection conn, TriggerQuery query) {
        String sql = getSelectTriggerBodySql(query);

        try (ResultSet rs = executeQuery(conn, sql)) {
            Optional<String> result;

            if (rs.next()) {
                result = Optional.ofNullable(rs.getString(1));
            } else {
                result = Optional.empty();
            }

            return result;
        } catch (CelestaException | SQLException e) {
            throw new CelestaException("Could't select body of trigger %s", query.getName());
        }
    }

    //TODO: Javadoc
    public void initDataForMaterializedView(Connection conn, MaterializedView mv) {
        this.ddlAdaptor.initDataForMaterializedView(conn, mv);
    }

    //TODO: Javadoc
    @Override
    public List<String> selectStaticStrings(
            List<String> data, String columnName, String orderByDirection) {

        int maxStringLength = data.stream().mapToInt(String::length).max().getAsInt();

        //prepare sql
        String sql = data.stream().map(
                str -> {
                    final String rowStr = prepareRowColumnForSelectStaticStrings(str, columnName, maxStringLength);
                    return String.format("SELECT %s %s", rowStr, constantFromSql());
                })
                .collect(Collectors.joining(" UNION ALL "));

        if (orderByDirection != null && !orderByDirection.isEmpty()) {
            sql = sql + " " + this.orderByForSelectStaticStrings(columnName, orderByDirection);
        }

        try (Connection conn = connectionPool.get();
             PreparedStatement ps = conn.prepareStatement(sql)
        ) {
            //fill preparedStatement
            AtomicInteger paramCounter = new AtomicInteger(1);
            data.forEach(
                    str -> {
                        try {
                            ps.setString(paramCounter.getAndIncrement(), str);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });

            //execute query and parse result
            try (ResultSet rs = ps.executeQuery()) {
                List<String> result = new ArrayList<>();

                while (rs.next()) {
                    String str = rs.getString(1);
                    result.add(str);
                }
                return result;
            }
        } catch (Exception e) {
            throw new CelestaException("Can't select static data", e);
        }
    }

    String orderByForSelectStaticStrings(String columnName, String orderByDirection) {
        return String.format("ORDER BY %s %s", columnName, orderByDirection);
    }

    //TODO: Javadoc
    @Override
    public int compareStrings(String left, String right) {

        List<String> comparisons = Arrays.asList("<", "=", ">");

        int maxStringLength = Math.max(left.length(), right.length());

        String sql = comparisons.stream()
                .map(comparison ->
                        "SELECT COUNT(*) "
                     + " FROM ( SELECT " + prepareRowColumnForSelectStaticStrings("?", "a", maxStringLength)
                                + " " + constantFromSql() + ") r "
                     + " WHERE a " + comparison + " ?"
                )
                .collect(Collectors.joining(" UNION ALL "));

        try (Connection conn = connectionPool.get();
             PreparedStatement ps = conn.prepareStatement(sql)
        ) {
            for (int i = 1; i < comparisons.size() * 2; i += 2) {
                ps.setString(i, left);
                ps.setString(i + 1, right);
            }

            try (ResultSet rs = ps.executeQuery()) {
                int result = -1;
                while (rs.next()) {
                    boolean compareResult = rs.getBoolean(1);
                    if (compareResult) {
                        break;
                    }
                    ++result;
                }
                return result;
            }

        } catch (Exception e) {
            throw new CelestaException("Can't compare strings", e);
        }
    }

    /**
     * Whether DB supports cortege comparing.
     *
     * @return
     */
    @Override
    public boolean supportsCortegeComparing() {
        return false;
    }

    /**
     * Drops primary key from the table by using known name of the primary key.
     *
     * @param conn   DB connection
     * @param t      Table
     * @param pkName name of the primary key
     */
    public void dropPk(Connection conn, TableElement t, String pkName) {
        ddlAdaptor.dropPk(conn, t, pkName);
    }

    /**
     * Updates a table column.
     *
     * @param conn   DB connection
     * @param c      Column to update
     * @param actual Actual column info
     */
    public void updateColumn(Connection conn, Column<?> c, DbColumnInfo actual) {
        ddlAdaptor.updateColumn(conn, c, actual);
    }

    @Override
    public ZonedDateTime prepareZonedDateTimeForParameterSetter(Connection conn, ZonedDateTime z) {
        return z;
    }

    // =========> END PUBLIC METHODS <=========

    // =========> PUBLIC ABSTRACT METHODS <=========

    /**
     * Returns navigable PreparedStatement by a filtered set of records.
     *
     * @param conn                  Connection
     * @param from                  From clause
     * @param orderBy               Sorting order (ascending or descending)
     * @param navigationWhereClause Navigable set condition (from current record)
     * @param fields                Fields of selection
     * @param offset                First record offset
     */
    public abstract PreparedStatement getNavigationStatement(
            Connection conn, FromClause from, String orderBy,
            String navigationWhereClause, Set<String> fields, long offset
    );

    /**
     * Checks if table exists in the DB.
     *
     * @param conn  DB connection
     * @param schema  schema name
     * @param name  table name
     * @return
     */
    public abstract boolean tableExists(Connection conn, String schema, String name);

    /**
     * Checks if trigger exists in the DB.
     *
     * @param conn  DB connection.
     * @param query  trigger query parameters
     * @return
     * @throws SQLException  thrown if resulting query fails
     */
    public abstract boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException;

    /**
     * Creates a PreparedStatement object for a SELECT statement containing at most one record.
     *
     * @param conn  DB connection
     * @param t  table
     * @param where  WHERE condition
     * @param fields  fields of selection
     * @return
     */
    public abstract PreparedStatement getOneRecordStatement(Connection conn, TableElement t,
                                                            String where, Set<String> fields);

    /**
     * Creates a PreparedStatement object for a SELECT statement of a single column containing
     * at most one record.
     *
     * @param conn  DB connection
     * @param c  column to select
     * @param where  WHERE condition
     * @return
     */
    public abstract PreparedStatement getOneFieldStatement(Connection conn, Column<?> c, String where);

    /**
     * Creates a PreparedStatement object for a DELETE statement for deleting a set of records that
     * satisfy a condition.
     *
     * @param conn  DB connection
     * @param t  table
     * @param where  condition
     * @return
     */
    public abstract PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where);

    /**
     * Creates a PreparedStatement object for an INSERT statement to insert a record into a table.
     *
     * @param conn  DB connection
     * @param t  table
     * @param nullsMask  null-flags (if set the corresponding field at n-th position becomes {@code null})
     * @param program  collects parameters that can be set with the query
     * @return
     */
    public abstract PreparedStatement getInsertRecordStatement(Connection conn, BasicTable t, boolean[] nullsMask,
                                                               List<ParameterSetter> program);
    /**
     * Returns current identity value for the table.
     *
     * @param conn  DB connection
     * @param t  table
     * @return
     */
    public abstract int getCurrentIdent(Connection conn, BasicTable t);

    /**
     * Creates a PreparedStatement object for a DELETE statement for deleting a set of records that
     * satisfy a condition.
     *
     * @param conn  DB connection
     * @param t  table
     * @param where  condition (can be {@code null})
     * @return
     */
    public abstract PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where);

    /**
     * Returns information on a column.
     *
     * @param conn  DB connection
     * @param c     column
     */
    public abstract DbColumnInfo getColumnInfo(Connection conn, Column<?> c);

    /**
     * Returns information on the primary key of a table.
     *
     * @param conn  DB connection
     * @param t     Table that the information on the primary key has to be returned from
     */
    public abstract DbPkInfo getPKInfo(Connection conn, TableElement t);

    /**
     * Returns information on the foreign keys from grain.
     *
     * @param conn  DB connection
     * @param g  grain name
     * @return  list where each item contain information on a separate foreign key
     */
    public abstract List<DbFkInfo> getFKInfo(Connection conn, Grain g);

    /**
     * Returns a set of indices referring to tables specified in the indicated grain.
     *
     * @param conn  DB connection
     * @param g     Grain the tables of which have to be traversed for the indices.
     */
    public abstract Map<String, DbIndexInfo> getIndices(Connection conn, Grain g);

    //TODO: Javadoc
    public abstract List<String> getParameterizedViewList(Connection conn, Grain g);

    /**
     * Returns process id of current database connection.
     *
     * @param conn  DB connection
     */
    public abstract int getDBPid(Connection conn);

    /**
     * Returns current database type. E.g. <b>H2</b>, <b>POSTGRESQL</b> etc.
     *
     * @return
     */
    public abstract DBType getType();

    /**
     * Retrieves next value from the sequence.
     *
     * @param conn  DB connection
     * @param s  sequence
     * @return
     */
    public abstract long nextSequenceValue(Connection conn, SequenceElement s);

    /**
     * Checks if sequence exists in the DB.
     *
     * @param conn  DB connection
     * @param schema  schema name
     * @param name  sequence name
     * @return
     */
    public abstract boolean sequenceExists(Connection conn, String schema, String name);

    /**
     * Returns information on a sequence.
     *
     * @param conn  DB connection
     * @param s  sequence
     * @return
     */
    public abstract DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s);
    // =========> END PUBLIC ABSTRACT METHODS <=========
}
