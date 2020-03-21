package ru.curs.celesta.dbutils.adaptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.constants.FireBirdConstants;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlGenerator;
import ru.curs.celesta.dbutils.adaptors.ddl.FirebirdDdlGenerator;
import ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;

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
import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.NamedElement;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.TableElement;
import ru.curs.celesta.score.validator.AnsiQuotedIdentifierParser;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.curs.celesta.dbutils.jdbc.SqlUtils.executeUpdate;

/**
 * FirebirdAdaptor.
 *
 * @author ioanngolovko
 */
public final class FirebirdAdaptor extends DBAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebirdAdaptor.class);

    private static final Pattern TABLE_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");

    private static final Pattern HEX_STRING = Pattern.compile("'([0-9A-F]+)'"); // TODO:: COPY PASTE

    private static final Pattern DATE_PATTERN = Pattern.compile("'(\\d\\d)\\.(\\d\\d)\\.(\\d\\d\\d\\d)'");

    private static final Pattern SEQUENCE_INFO_PATTERN =
            Pattern.compile("/\\* INCREMENT_BY = (.*), MINVALUE = (.*), MAXVALUE = (.*), CYCLE = (.*) \\*/");

    private static final String CUR_VALUE_PROC_POSTFIX = "curValueProc";
    private static final String NEXT_VALUE_PROC_POSTFIX = "nextValueProc";

    public FirebirdAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new FirebirdDdlGenerator(this);
    }

    @Override
    String getLimitedSQL(FromClause from, String whereClause, String orderBy, long offset, long rowCount,
                         Set<String> fields) {
        if (offset == 0 && rowCount == 0) {
            throw new IllegalArgumentException();
        }

        final String sql;

        String firstSql = "";
        if (rowCount != 0) {
            firstSql = String.format("FIRST %s", rowCount);
        }

        String sqlwhere = "".equals(whereClause) ? "" : " WHERE " + whereClause;

        final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);

        sql = String.format(
                "SELECT %s SKIP %d %s FROM %s %s ORDER BY %s",
                firstSql,
                offset,
                fieldList,
                from.getExpression(),
                sqlwhere,
                orderBy
        );

        return sql;


    }

    @Override
    String getSelectTriggerBodySql(TriggerQuery query) {
        String sql = String.format("SELECT RDB$TRIGGER_SOURCE FROM RDB$TRIGGERS "
                        + "WHERE RDB$TRIGGER_NAME = '%s' AND RDB$RELATION_NAME = '%s_%s'",
                query.getName(), query.getSchema(), query.getTableName());

        return sql;
    }

    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) \n"
                + "FROM RDB$RELATIONS RDB$RELATIONS \n"
                + "WHERE RDB$SYSTEM_FLAG = 0";

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    @Override
    void createSchemaIfNotExists(Connection conn, String name) {

    }

    @Override
    public PreparedStatement getNavigationStatement(Connection conn, FromClause from, String orderBy,
                                                    String navigationWhereClause, Set<String> fields, long offset) {
        if (navigationWhereClause == null) {
            throw new IllegalArgumentException();
        }
        StringBuilder w = new StringBuilder(navigationWhereClause);
        final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);
        boolean useWhere = w.length() > 0;
        if (orderBy.length() > 0) {
            w.append(" order by " + orderBy);
        }
        String sql = String.format("SELECT FIRST 1 SKIP %d %s FROM  %s %s;", offset == 0 ? 0 : offset - 1,
                fieldList,
                from.getExpression(), useWhere ? " where " + w : w);
        LOGGER.trace(sql);
        return prepareStatement(conn, sql);
    }

    @Override
    public boolean tableExists(Connection conn, String schema, String name) {
        String sql = String.format(
                "SELECT count(*)%n"
                        + "FROM RDB$RELATIONS%n"
                        + "WHERE RDB$RELATION_NAME = '%s_%s'",
                schema,
                name
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();
            boolean result = rs.getInt(1) > 0;
            rs.close();
            return result;
        } catch (Exception e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException {
        String sql = String.format(
                "SELECT count(*) FROM RDB$TRIGGERS%n"
                        + "WHERE %n"
                        + "  RDB$TRIGGER_NAME = '%s' AND RDB$RELATION_NAME = '%s_%s'",
                query.getName(),
                query.getSchema(),
                query.getTableName()
        );

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            boolean result = rs.getInt(1) > 0;
            rs.close();
            return result;
        }
    }

    @Override
    public Set<String> getColumns(Connection conn, TableElement t) {
        Set<String> result = new LinkedHashSet<>();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(null,
                    null,
                    t.getGrain().getName() + "_" + t.getName(), null)) {
                while (rs.next()) {
                    String rColumnName = rs.getString(COLUMN_NAME);
                    result.add(rColumnName);
                }
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
        return result;
    }

    @Override
    public PreparedStatement getOneRecordStatement(Connection conn, TableElement t, String where, Set<String> fields) {
        final String fieldList = getTableFieldsListExceptBlobs((DataGrainElement) t, fields);
        String sql = String.format(
                "select first 1 %s from %s where %s;",
                fieldList,
                tableString(t.getGrain().getName(), t.getName()),
                where
        );

        PreparedStatement result = prepareStatement(conn, sql);
        LOGGER.trace("{}", result);
        return result;
    }

    @Override
    public PreparedStatement getOneFieldStatement(Connection conn, Column<?> c, String where) {
        TableElement t = c.getParentTable();

        String sql = String.format(
                "select first 1 %s from %s where %s;",
                c.getQuotedName(),
                tableString(t.getGrain().getName(), t.getName()),
                where
        );

        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) {
        // TODO:: COPY PASTE
        // Готовим запрос на удаление
        String sql = String.format("delete from " + tableString(t.getGrain().getName(), t.getName()) + " %s;",
                where.isEmpty() ? "" : "where " + where);
        try {
            return conn.prepareStatement(sql);
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
    public int getCurrentIdent(Connection conn, BasicTable t) {

        IntegerColumn idColumn = t.getPrimaryKey().values().stream()
                .filter(c -> c instanceof IntegerColumn)
                .map(c -> (IntegerColumn) c)
                .filter(ic -> ic.getSequence() != null)
                .findFirst().get();

        final SequenceElement s = idColumn.getSequence();
        String curValueProcName = sequenceCurValueProcString(s.getGrain().getName(), s.getName());
        String sql = String.format("EXECUTE PROCEDURE %s(null)", curValueProcName);

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) {
        // TODO:: COPY PASTE
        String sql = String.format("delete from " + tableString(t.getGrain().getName(), t.getName()) + " where %s;",
                where);
        return prepareStatement(conn, sql);
    }

    @Override
    public DbColumnInfo getColumnInfo(Connection conn, Column<?> c) {
        String sql = String.format(
                "SELECT r.RDB$FIELD_NAME AS column_name,%n"
                        + "        r.RDB$DESCRIPTION AS field_description,%n"
                        + "        r.RDB$NULL_FLAG AS nullable,%n"
                        + "        f.RDB$FIELD_LENGTH AS column_length,%n"
                        + "        f.RDB$FIELD_PRECISION AS column_precision,%n"
                        + "        f.RDB$FIELD_SCALE AS column_scale,%n"
                        + "        CASE f.RDB$FIELD_TYPE%n"
                        + "          WHEN 261 THEN 'BLOB'%n"
                        + "          WHEN 14 THEN 'CHAR'%n"
                        + "          WHEN 40 THEN 'CSTRING'%n"
                        + "          WHEN 11 THEN 'D_FLOAT'%n"
                        + "          WHEN 27 THEN 'DOUBLE PRECISION'%n"
                        + "          WHEN 10 THEN 'FLOAT'%n"
                        + "          WHEN 16 THEN 'BIGINT'%n"
                        + "          WHEN 8 THEN 'INTEGER'%n"
                        + "          WHEN 9 THEN 'QUAD'%n"
                        + "          WHEN 7 THEN 'SMALLINT'%n"
                        + "          WHEN 12 THEN 'DATE'%n"
                        + "          WHEN 13 THEN 'TIME'%n"
                        + "          WHEN 35 THEN 'TIMESTAMP'%n"
                        + "          WHEN 29 THEN 'TIMESTAMP WITH TIME ZONE'%n"
                        + "          WHEN 37 THEN 'VARCHAR'%n"
                        + "          ELSE 'UNKNOWN'%n"
                        + "        END AS column_type,%n"
                        + "        f.RDB$FIELD_SUB_TYPE AS column_subtype%n"
                        + "   FROM RDB$RELATION_FIELDS r%n"
                        + "   LEFT JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME%n"
                        + "   LEFT JOIN RDB$COLLATIONS coll ON f.RDB$COLLATION_ID = coll.RDB$COLLATION_ID%n"
                        + "   LEFT JOIN RDB$CHARACTER_SETS cset ON f.RDB$CHARACTER_SET_ID = cset.RDB$CHARACTER_SET_ID%n"
                        + "  WHERE r.RDB$RELATION_NAME='%s_%s' AND r.RDB$FIELD_NAME = '%s'",
                c.getParentTable().getGrain().getName(),
                c.getParentTable().getName(),
                c.getName()
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            if (rs.next()) {
                DbColumnInfo result = new DbColumnInfo();

                result.setName(rs.getString("column_name").trim());
                String columnType = rs.getString("column_type").trim();
                Integer columnSubType = rs.getInt("column_subtype");

                if (
                        ("BIGINT".equals(columnType) || "INTEGER".equals(columnType))
                                && Integer.valueOf(2).equals(columnSubType)
                ) {
                    result.setType(DecimalColumn.class);
                    result.setLength(rs.getInt("column_precision"));
                    result.setScale(Math.abs(rs.getInt("column_scale")));
                } else if ("BLOB".equals(columnType) && Integer.valueOf(1).equals(columnSubType)) {
                    result.setType(StringColumn.class);
                    result.setMax(true);
                } else {
                    for (Class<? extends Column<?>> cc : COLUMN_CLASSES) {
                        if (getColumnDefiner(cc).dbFieldType().equalsIgnoreCase(columnType)) {
                            result.setType(cc);
                            break;
                        }
                    }
                }

                result.setNullable(rs.getInt("nullable") != 1);

                if (result.getType() == StringColumn.class) {
                    result.setLength(rs.getInt("column_length"));
                }

                this.processDefaults(conn, c, result);

                return result;
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new CelestaException(e);
        }

    }

    private void processDefaults(Connection conn, Column<?> c, DbColumnInfo dbColumnInfo) throws SQLException {
        String defaultValue = null;

        TableElement te = c.getParentTable();
        Grain g = te.getGrain();

        String sql = String.format(
                "SELECT r.RDB$DEFAULT_SOURCE AS column_default_value%n"
                        + "   FROM RDB$RELATION_FIELDS r%n"
                        + "   WHERE r.RDB$RELATION_NAME='%s_%s' AND r.RDB$FIELD_NAME = '%s'",
                c.getParentTable().getGrain().getName(),
                c.getParentTable().getName(),
                c.getName()
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();

            String defaultSource = rs.getString(1);

            if (defaultSource == null) {
                if (IntegerColumn.class.equals(dbColumnInfo.getType())) {
                    String triggerName = SchemalessFunctions.generateSequenceTriggerName((IntegerColumn) c);
                    sql = String.format(
                        "SELECT proc.RDB$DEPENDED_ON_NAME %n "
                                + "FROM RDB$DEPENDENCIES tr%n "
                                + "JOIN RDB$DEPENDENCIES proc ON tr.RDB$DEPENDED_ON_NAME = proc.RDB$DEPENDENT_NAME%n "
                                + "WHERE tr.RDB$DEPENDENT_NAME = '%s' AND tr.RDB$DEPENDENT_TYPE = 2 "
                                + "AND tr.RDB$DEPENDED_ON_TYPE = 5%n "
                                + "AND proc.RDB$DEPENDENT_TYPE = 5 AND proc.RDB$DEPENDED_ON_TYPE = 14",
                        triggerName
                    );

                    try (ResultSet sequenceRs = SqlUtils.executeQuery(conn, sql)) {
                        if (sequenceRs.next()) {
                            String sequenceName = sequenceRs.getString(1).trim();
                            defaultValue = "NEXTVAL("
                                    // TODO: score sequence name could be spoiled here because of name limitation
                                    + sequenceName.replace(g.getName() + "_", "")
                                    + ")";
                        }
                    }
                }
            } else {
                defaultValue = defaultSource.replace("default", "").trim();

                if (BooleanColumn.class.equals(dbColumnInfo.getType())) {
                    defaultValue = "0".equals(defaultValue) ? "'FALSE'" : "'TRUE'";
                } else if (DateTimeColumn.class.equals(dbColumnInfo.getType())) {
                    if (FireBirdConstants.CURRENT_TIMESTAMP.equalsIgnoreCase(defaultValue)) {
                        defaultValue = "GETDATE()";
                    } else {
                        Matcher m = DATE_PATTERN.matcher(defaultValue);
                        if (m.find()) {
                            defaultValue = String.format("'%s%s%s'", m.group(3), m.group(2), m.group(1));
                        }
                    }
                } else if (BinaryColumn.class.equals(dbColumnInfo.getType())) {
                    Matcher m = HEX_STRING.matcher(defaultValue);
                    if (m.find()) {
                        defaultValue = "0x" + m.group(1);
                    }
                }
            }

        }

        if (defaultValue != null) {
            dbColumnInfo.setDefaultValue(defaultValue);
        }
    }

    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        String sql = String.format(
                "select%n"
                        + "    ix.rdb$index_name as pk_name,%n"
                        + "    sg.rdb$field_name as column_name%n"
                        + " from%n"
                        + "    rdb$indices ix%n"
                        + "    left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name%n"
                        + "    left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name%n"
                        + " where%n"
                        + "    rc.rdb$constraint_type = 'PRIMARY KEY' AND rc.rdb$relation_name = '%s_%s'",
                t.getGrain().getName(),
                t.getName()
        );

        DbPkInfo result = new DbPkInfo(this);

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                if (result.getName() == null) {
                    String pkName = rs.getString("pk_name").trim();
                    result.setName(pkName);
                }

                String columnName = rs.getString("column_name").trim();
                result.addColumnName(columnName);
            }

        } catch (Exception e) {
            throw new CelestaException(e);
        }

        return result;
    }

    @Override
    public List<DbFkInfo> getFKInfo(Connection conn, Grain g) {
        String sql = String.format(
            "SELECT"
                    + "    detail_relation_constraints.RDB$RELATION_NAME as table_name%n"
                    + "    , detail_relation_constraints.RDB$CONSTRAINT_NAME as constraint_name%n"
                    + "    , ref_constraints.RDB$UPDATE_RULE as update_rule%n"
                    + "    , ref_constraints.RDB$DELETE_RULE as delete_rule%n"
                    + "    , detail_index_segments.rdb$field_name AS column_name%n"
                    + "    , master_relation_constraints.rdb$relation_name AS ref_table_name%n"
                    + "FROM%n"
                    + "    rdb$relation_constraints detail_relation_constraints%n"
                    + "    JOIN rdb$index_segments detail_index_segments ON "
                    + "      detail_relation_constraints.rdb$index_name = detail_index_segments.rdb$index_name %n"
                    + "    JOIN rdb$ref_constraints ref_constraints ON "
                    + "      detail_relation_constraints.rdb$constraint_name = ref_constraints.rdb$constraint_name%n"
                    + "    JOIN rdb$relation_constraints master_relation_constraints ON "
                    + "      ref_constraints.rdb$const_name_uq = master_relation_constraints.rdb$constraint_name%n"
                    + "WHERE%n"
                    + "    detail_relation_constraints.rdb$constraint_type = 'FOREIGN KEY'%n"
                    + "    AND detail_relation_constraints.rdb$relation_name like '%s@_%%' escape '@'%n"
                    + "ORDER BY table_name, constraint_name, detail_index_segments.rdb$field_position;",
            g.getName()
        );

        Map<String, DbFkInfo> fks = new HashMap<>();

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                String fkName = rs.getString("constraint_name").trim();

                String fullTableName = rs.getString("table_name").trim();
                String tableName = convertNameFromDb(fullTableName, g);

                String fullRefTableName = rs.getString("ref_table_name").trim();
                String refGrainName = fullRefTableName.substring(0, fullRefTableName.indexOf("_"));
                String refTableName = fullRefTableName.substring(refGrainName.length() + 1);

                FKRule updateRule = getFKRule(rs.getString("update_rule").trim());
                FKRule deleteRule = getFKRule(rs.getString("delete_rule").trim());

                String columnName = rs.getString("column_name").trim();

                fks.computeIfAbsent(
                        fkName,
                        key -> {
                            DbFkInfo dfi = new DbFkInfo(fkName);

                            dfi.setTableName(tableName);
                            dfi.setRefGrainName(refGrainName);
                            dfi.setRefTableName(refTableName);
                            dfi.setDeleteRule(deleteRule);
                            dfi.setUpdateRule(updateRule);

                            return dfi;
                        }
                ).getColumnNames().add(columnName);

            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }

        return new ArrayList<>(fks.values());
    }


    @Override
    public Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) {
        String sql = String.format(
                "SELECT RDB$INDICES.RDB$INDEX_NAME as indexname, RDB$INDICES.RDB$RELATION_NAME as tablename, "
                        + "RDB$INDEX_SEGMENTS.RDB$FIELD_NAME AS columnname%n"
                        + "FROM RDB$INDEX_SEGMENTS%n"
                        + "LEFT JOIN RDB$INDICES "
                        + " ON RDB$INDICES.RDB$INDEX_NAME = RDB$INDEX_SEGMENTS.RDB$INDEX_NAME%n"
                        + "LEFT JOIN RDB$RELATION_CONSTRAINTS "
                        + " ON RDB$RELATION_CONSTRAINTS.RDB$INDEX_NAME = RDB$INDEX_SEGMENTS.RDB$INDEX_NAME%n"
                        + "WHERE RDB$RELATION_CONSTRAINTS.RDB$CONSTRAINT_TYPE IS NULL "
                        + "AND RDB$INDICES.RDB$RELATION_NAME like '%s@_%%' escape '@'%n"
                        + "ORDER BY RDB$INDEX_SEGMENTS.RDB$FIELD_POSITION",
                g.getName()
        );

        Map<String, DbIndexInfo> result = new HashMap<>();

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            DbIndexInfo i = null;
            while (rs.next()) {
                String tabName = rs.getString("tablename").trim();
                tabName = convertNameFromDb(tabName, g);
                String indName = rs.getString("indexname").trim();
                indName = convertNameFromDb(indName, g);

                if (i == null || !i.getTableName().equals(tabName) || !i.getIndexName().equals(indName)) {
                    i = new DbIndexInfo(tabName, indName);
                    result.put(indName, i);
                }
                i.getColumnNames().add(rs.getString("columnname").trim());

            }
        } catch (Exception e) {
            throw new CelestaException(e);
        }

        return result;
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        List<String> result = new ArrayList<>();

        // TODO: grain names may be cut, so reconsider the following
        String sql = String.format(
                "SELECT RDB$PROCEDURE_NAME%n"
                        + "FROM RDB$PROCEDURES%n"
                        + "WHERE RDB$PROCEDURE_NAME LIKE '%s@_%%' escape '@' %n"
                        + "AND RDB$PROCEDURE_NAME NOT LIKE '%%" + CUR_VALUE_PROC_POSTFIX + "%%' escape '@' %n"
                        + "AND RDB$PROCEDURE_NAME NOT LIKE '%%" + NEXT_VALUE_PROC_POSTFIX + "%%' escape '@' %n",
                g.getName()
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                String dbName = rs.getString(1).trim();
                result.add(convertNameFromDb(dbName, g));
            }
        } catch (Exception e) {
            throw new CelestaException(e);
        }

        return result;
    }

    @Override
    public int getDBPid(Connection conn) {
        try (
                ResultSet rs = SqlUtils.executeQuery(
                        conn, "SELECT MON$SERVER_PID as pid FROM MON$ATTACHMENTS")
        ) {
            if (rs.next()) {
                return rs.getInt("pid");
            }
        } catch (SQLException e) {
            // do nothing
        }

        return 0;
    }

    @Override
    public DBType getType() {
        return DBType.FIREBIRD;
    }

    @Override
    public long nextSequenceValue(Connection conn, SequenceElement s) {
        String nextValueProcName = sequenceNextValueProcString(s.getGrain().getName(), s.getName());

        String sql = String.format("EXECUTE PROCEDURE %s", nextValueProcName);

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new CelestaException(
                    "Can't get current value of sequence " + tableString(s.getGrain().getName(), s.getName()), e
            );
        }
    }

    @Override
    public ZonedDateTime prepareZonedDateTimeForParameterSetter(Connection conn, ZonedDateTime z) {
        return z.withZoneSameInstant(ZoneId.systemDefault());
    }

    @Override
    public void dropSequence(Connection conn, SequenceElement s) {
        String nextValueProcName = sequenceNextValueProcString(s.getGrain().getName(), s.getName());
        String sql = String.format("DROP PROCEDURE %s", nextValueProcName);
        executeUpdate(conn, sql);

        String curValueProcName = sequenceCurValueProcString(s.getGrain().getName(), s.getName());
        sql = String.format("DROP PROCEDURE %s", curValueProcName);
        executeUpdate(conn, sql);

        super.dropSequence(conn, s);
    }

    public static String sequenceCurValueProcString(String schemaName, String sequenceName) {
        return sequenceCurValueProcString(schemaName, sequenceName, true);
    }

    private static String sequenceCurValueProcString(String schemaName, String sequenceName, boolean isQuoted) {
        return sequenceProcString(schemaName, sequenceName, "_" + CUR_VALUE_PROC_POSTFIX, isQuoted);
    }

    public static String sequenceNextValueProcString(String schemaName, String sequenceName) {
        return sequenceNextValueProcString(schemaName, sequenceName, true);
    }

    private static String sequenceNextValueProcString(String schemaName, String sequenceName, boolean isQuoted) {
        return sequenceProcString(schemaName, sequenceName, "_" + NEXT_VALUE_PROC_POSTFIX, isQuoted);
    }

    private static String sequenceProcString(
            String schemaName, String sequenceName, String procPostfix, boolean isQuoted) {

        StringBuilder sb = new StringBuilder(
                NamedElement.limitName(getSchemaUnderscoreNameTemplate(schemaName, sequenceName), procPostfix));
        if (isQuoted) {
            sb.insert(0, '"').append('"');
        }

        return sb.toString();
    }

    @Override
    public boolean sequenceExists(Connection conn, String schema, String name) {
        String sql = String.format("SELECT * FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = '%s'",
                sequenceString(schema, name, false));

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            return rs.next();
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        String nextValueProcName = sequenceNextValueProcString(s.getGrain().getName(), s.getName(), false);

        String sql = String.format("SELECT RDB$PROCEDURE_SOURCE FROM RDB$PROCEDURES "
                        + "WHERE RDB$PROCEDURE_NAME = '%s'",
                nextValueProcName);

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();
            String body = rs.getString(1);

            Matcher matcher = SEQUENCE_INFO_PATTERN.matcher(body);

            matcher.find();

            DbSequenceInfo dbSequenceInfo = new DbSequenceInfo();
            dbSequenceInfo.setIncrementBy(Long.parseLong(matcher.group(1)));
            dbSequenceInfo.setMinValue(Long.parseLong(matcher.group(2)));
            dbSequenceInfo.setMaxValue(Long.parseLong(matcher.group(3)));
            dbSequenceInfo.setCycle(Boolean.parseBoolean(matcher.group(4)));

            return dbSequenceInfo;

        } catch (Exception e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public boolean nullsFirst() {
        return false;
    }

    @Override
    public String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields,
                                    List<String> otherFields, String whereForOtherTable) {
        // TODO: COPY PASTE (Mssql)
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

        String result = String.format(template, otherTableStr, sb.toString(), whereForOtherTable);
        return result;
    }


    @Override
    public void createSysObjects(Connection conn, String sysSchemaName) {
        String versionCheckErrorSql =
                "CREATE OR ALTER EXCEPTION VERSION_CHECK_ERROR 'record version check failure'";
        String sequenceOverflowErrorSql =
                "CREATE OR ALTER EXCEPTION SEQUENCE_OVERFLOW_ERROR 'sequence overflow failure'";

        try {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(versionCheckErrorSql);
                stmt.executeUpdate(sequenceOverflowErrorSql);
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not create or alter versioncheck exception: %s", e.getMessage());
        }
    }

    @Override
    public List<String> getViewList(Connection conn, Grain g) {
        List<String> result = new ArrayList<>();

        String sql = String.format(
                "select rdb$relation_name%n"
                        + "from rdb$relations%n"
                        + "where rdb$view_blr is not null %n"
                        + "and (rdb$system_flag is null or rdb$system_flag = 0)"
                        + "and rdb$relation_name like '%s@_%%' escape '@'",
                g.getName()
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                String dbName = rs.getString(1).trim();
                result.add(convertNameFromDb(dbName, g));
            }
        } catch (Exception e) {
            throw new CelestaException(e);
        }

        return result;
    }

    // TODO:: Start of copy-pasting from OraAdaptor
    @Override
    public String tableString(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder(getSchemaUnderscoreNameTemplate(schemaName, tableName));
        sb.insert(0, '"').append('"');

        return sb.toString();
    }

    @Override
    public String pkConstraintString(TableElement tableElement) {
        return NamedElement.limitName(
                tableElement.getPkConstraintName() + "_" + tableElement.getGrain().getName());
    }

    @Override
    String constantFromSql() {
        return "FROM RDB$DATABASE";
    }

    @Override
    String prepareRowColumnForSelectStaticStrings(String value, String colName, int maxStringLength) {
        return String.format("CAST(? as varchar(%d)) as %s", maxStringLength, colName);
    }

    @Override
    String orderByForSelectStaticStrings(String columnName, String orderByDirection) {
        return String.format("ORDER BY 1 %s", orderByDirection);
    }

    private static String getSchemaUnderscoreNameTemplate(String schemaName, String name) {
        return stripNameFromQuotes(schemaName) + "_" + stripNameFromQuotes(name);
    }

    private static String stripNameFromQuotes(String name) {
        return name.startsWith("\"") ? name.substring(1, name.length() - 1) : name;
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

    // TODO:: End of copy-pasting from OraAdaptor
}
