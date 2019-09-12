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
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;
import ru.curs.celesta.score.validator.AnsiQuotedIdentifierParser;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class FirebirdAdaptor extends DBAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebirdAdaptor.class);

    private static final Pattern TABLE_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");

    private static final Pattern HEX_STRING = Pattern.compile("'([0-9A-F]+)'"); // TODO:: COPY PASTE

    public static final Pattern DATE_PATTERN = Pattern.compile("'(\\d\\d)\\.(\\d\\d)\\.(\\d\\d\\d\\d)'");

    public FirebirdAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new FirebirdDdlGenerator(this);
    }

    @Override
    String getLimitedSQL(FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields) {
        if (offset == 0 && rowCount == 0) {
            throw new IllegalArgumentException();
        }

        final String sql;

        String first = "";
        String offsetSql = "";

        if (rowCount != 0) {
            first = String.format("FIRST %s", rowCount);
        }

        if (offset != 0) {
            offsetSql = String.format("OFFSET %s ROWS", offset);
        }

        String sqlwhere = "".equals(whereClause) ? "" : " WHERE " + whereClause;

        final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);

        sql = String.format("SELECT %s %s FROM %s", first, fieldList,
            from.getExpression()) + sqlwhere + " ORDER BY " + orderBy + " " + offsetSql;

        return sql;


    }

    @Override
    String getSelectTriggerBodySql(TriggerQuery query) {
        return null;
    }

    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) \n" +
            "FROM RDB$RELATIONS RDB$RELATIONS \n" +
            "WHERE RDB$SYSTEM_FLAG = 0";

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    @Override
    void createSchemaIfNotExists(Connection conn, String name) {

    }

    @Override
    public PreparedStatement getNavigationStatement(Connection conn, FromClause from, String orderBy, String navigationWhereClause, Set<String> fields, long offset) {
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
            "SELECT count(*)\n" +
                "FROM RDB$RELATIONS\n" +
                "WHERE RDB$RELATION_NAME = '%s_%s'",
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
            "SELECT count(*) FROM RDB$TRIGGERS\n" +
                "WHERE \n" +
                "  RDB$TRIGGER_NAME = '%s' AND RDB$RELATION_NAME = '%s_%s'",
            query.getName(),
            query.getSchema(),
            query.getTableName()
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

    @Override
    public Set<String> getColumns(Connection conn, TableElement t) {
        Set<String> result = new LinkedHashSet<>();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(null,
                null,
                t.getGrain().getName() + "_" + t.getName(), null);
            try {
                while (rs.next()) {
                    String rColumnName = rs.getString(COLUMN_NAME);
                    result.add(rColumnName);
                }
            } finally {
                rs.close();
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
    public PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) {
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
            PreparedStatement result = conn.prepareStatement(sql);
            return result;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    public PreparedStatement getInsertRecordStatement(Connection conn, BasicTable t, boolean[] nullsMask, List<ParameterSetter> program) {
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
        for (Column c : t.getColumns().values()) {
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

        final String sequenceName = sequenceString(t.getGrain().getName(), idColumn.getSequence().getName());
        String sql = String.format("SELECT GEN_ID(%s, 0) FROM RDB$DATABASE", sequenceName);

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
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
    public DbColumnInfo getColumnInfo(Connection conn, Column c) {
        String sql = String.format(
            "SELECT r.RDB$FIELD_NAME AS column_name,\n" +
                "        r.RDB$DESCRIPTION AS field_description,\n" +
                "        r.RDB$NULL_FLAG AS nullable,\n" +
                "        f.RDB$FIELD_LENGTH AS column_length,\n" +
                "        f.RDB$FIELD_PRECISION AS column_precision,\n" +
                "        f.RDB$FIELD_SCALE AS column_scale,\n" +
                "        CASE f.RDB$FIELD_TYPE\n" +
                "          WHEN 261 THEN 'BLOB'\n" +
                "          WHEN 14 THEN 'CHAR'\n" +
                "          WHEN 40 THEN 'CSTRING'\n" +
                "          WHEN 11 THEN 'D_FLOAT'\n" +
                "          WHEN 27 THEN 'DOUBLE'\n" +
                "          WHEN 10 THEN 'FLOAT'\n" +
                "          WHEN 16 THEN 'BIGINT'\n" +
                "          WHEN 8 THEN 'INTEGER'\n" +
                "          WHEN 9 THEN 'QUAD'\n" +
                "          WHEN 7 THEN 'SMALLINT'\n" +
                "          WHEN 12 THEN 'DATE'\n" +
                "          WHEN 13 THEN 'TIME'\n" +
                "          WHEN 35 THEN 'TIMESTAMP'\n" +
                "          WHEN 29 THEN 'TIMESTAMP WITH TIME ZONE'\n" +
                "          WHEN 37 THEN 'VARCHAR'\n" +
                "          ELSE 'UNKNOWN'\n" +
                "        END AS column_type,\n" +
                "        f.RDB$FIELD_SUB_TYPE AS column_subtype\n" +
                "   FROM RDB$RELATION_FIELDS r\n" +
                "   LEFT JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME\n" +
                "   LEFT JOIN RDB$COLLATIONS coll ON f.RDB$COLLATION_ID = coll.RDB$COLLATION_ID\n" +
                "   LEFT JOIN RDB$CHARACTER_SETS cset ON f.RDB$CHARACTER_SET_ID = cset.RDB$CHARACTER_SET_ID\n" +
                "  WHERE r.RDB$RELATION_NAME='%s_%s' AND r.RDB$FIELD_NAME = '%s'",
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

    private void processDefaults(Connection conn, Column c, DbColumnInfo dbColumnInfo) throws SQLException {
        String defaultValue = null;

        TableElement te = c.getParentTable();
        Grain g = te.getGrain();

        String sql = String.format(
            "SELECT r.RDB$DEFAULT_SOURCE AS column_default_value\n" +
                "   FROM RDB$RELATION_FIELDS r\n" +
                "   WHERE r.RDB$RELATION_NAME='%s_%s' AND r.RDB$FIELD_NAME = '%s'",
            c.getParentTable().getGrain().getName(),
            c.getParentTable().getName(),
            c.getName()
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            rs.next();

            String defaultSource = rs.getString(1);

            if (defaultSource == null) {
                if (IntegerColumn.class.equals(dbColumnInfo.getType())) {

                    String triggerName = String.format(
                        //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                        "%s_%s_%s_seq_trigger",
                        te.getGrain().getName(),
                        te.getName(),
                        c.getName()
                    );

                    sql = String.format(
                        "SELECT RDB$DEPENDED_ON_NAME\n " +
                            "FROM RDB$DEPENDENCIES \n " +
                            "WHERE RDB$DEPENDENT_NAME = '%s' AND RDB$DEPENDENT_TYPE = 2 AND RDB$DEPENDED_ON_TYPE = 14",
                        triggerName
                    );

                    try (ResultSet sequenceRs = SqlUtils.executeQuery(conn, sql)) {
                        if (sequenceRs.next()) {
                            String sequenceName = sequenceRs.getString(1).trim();
                            defaultValue = "NEXTVAL(" + sequenceName.replace(g.getName() + "_", "") + ")";
                        }
                    }
                }
            } else {
                defaultValue = defaultSource.replace("default", "").trim();

                if (BooleanColumn.class.equals(dbColumnInfo.getType())) {
                    defaultValue = "0".equals(defaultValue) ? "'FALSE'" : "'TRUE'";
                } else if (DateTimeColumn.class.equals(dbColumnInfo.getType())) {
                    if ("current_timestamp".equalsIgnoreCase(defaultValue)) {
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


    private String modifyDefault(DbColumnInfo ci, String defaultBody) {
        String result = defaultBody;

        if (DateTimeColumn.class == ci.getType()) {
            if (FireBirdConstants.CURRENT_TIMESTAMP.equalsIgnoreCase(defaultBody)) {
                result = "GETDATE()";
            } else {
                Matcher m = DATE_PATTERN.matcher(defaultBody);
                m.find();
                result = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
            }
        }

        return result;
    }


    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        String sql = String.format(
            "select\n" +
                "    ix.rdb$index_name as pk_name,\n" +
                "    sg.rdb$field_name as column_name\n" +
                " from\n" +
                "    rdb$indices ix\n" +
                "    left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name\n" +
                "    left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name\n" +
                " where\n" +
                "    rc.rdb$constraint_type = 'PRIMARY KEY' AND rc.rdb$relation_name = '%s_%s'",
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
            "select DISTINCT relc.RDB$RELATION_NAME as table_name, relc.RDB$CONSTRAINT_NAME as constraint_name, " +
                "refc.RDB$UPDATE_RULE as update_rule, refc.RDB$DELETE_RULE as delete_rule, " +
                "d1.RDB$FIELD_NAME as column_name, d2.RDB$DEPENDED_ON_NAME AS ref_table_name \n" +
                "FROM RDB$INDEX_SEGMENTS inds \n" +
                "LEFT JOIN RDB$RELATION_CONSTRAINTS relc ON relc.RDB$INDEX_NAME = inds.RDB$INDEX_NAME\n" +
                "LEFT JOIN RDB$REF_CONSTRAINTS refc ON relc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME \n" +
                "LEFT JOIN RDB$DEPENDENCIES d1 ON d1.RDB$DEPENDED_ON_NAME = relc.RDB$RELATION_NAME \n" +
                "LEFT JOIN RDB$DEPENDENCIES d2 ON d1.RDB$DEPENDENT_NAME = d2.RDB$DEPENDENT_NAME \n" +
                "WHERE relc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY' AND relc.RDB$RELATION_NAME like '%s@_%%' escape '@' " +
                "AND d1.RDB$DEPENDED_ON_NAME <> d2.RDB$DEPENDED_ON_NAME " +
                "AND d1.RDB$FIELD_NAME <> d2.RDB$FIELD_NAME \n" +
                "ORDER BY inds.RDB$FIELD_POSITION",
            g.getName()
        );


        Map<String, DbFkInfo> fks = new HashMap<>();

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                String fkName = rs.getString("constraint_name").trim();

                String fullTableName = rs.getString("table_name").trim();
                String tableName = convertNameFromDb(fullTableName, g);

                String fullRefTableName = rs.getString("ref_table_name").trim();
                String refTableName = convertNameFromDb(fullRefTableName, g);
                String refGrainName = fullRefTableName.substring(0, fullRefTableName.indexOf(refTableName) - 1);

                FKRule updateRule = getFKRule(rs.getString("update_rule").trim());
                FKRule deleteRule = getFKRule(rs.getString("delete_rule").trim());

                String columnName = rs.getString("column_name").trim();

                fks.computeIfAbsent(
                    fkName,
                    (key) -> {
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
            "SELECT RDB$INDICES.RDB$INDEX_NAME as indexname, RDB$INDICES.RDB$RELATION_NAME as tablename, " +
                "RDB$INDEX_SEGMENTS.RDB$FIELD_NAME AS columnname\n" +
                "FROM RDB$INDEX_SEGMENTS\n" +
                "LEFT JOIN RDB$INDICES " +
                " ON RDB$INDICES.RDB$INDEX_NAME = RDB$INDEX_SEGMENTS.RDB$INDEX_NAME\n" +
                "LEFT JOIN RDB$RELATION_CONSTRAINTS " +
                " ON RDB$RELATION_CONSTRAINTS.RDB$INDEX_NAME = RDB$INDEX_SEGMENTS.RDB$INDEX_NAME\n" +
                "WHERE RDB$RELATION_CONSTRAINTS.RDB$CONSTRAINT_TYPE IS NULL " +
                "AND RDB$INDICES.RDB$RELATION_NAME like '%s@_%%' escape '@'\n" +
                "ORDER BY RDB$INDEX_SEGMENTS.RDB$FIELD_POSITION",
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

        String sql = String.format("select RDB$PROCEDURE_NAME\n" +
            "from RDB$PROCEDURES\n" +
            "where RDB$PROCEDURE_NAME like '%s@_%%' escape '@'", g.getName());

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
        try (ResultSet rs = SqlUtils.executeQuery(conn, "SELECT MON$SERVER_PID as pid \n" +
            "  FROM MON$ATTACHMENTS")) {
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
        return 0;
    }

    @Override
    public boolean sequenceExists(Connection conn, String schema, String name) {
        String sql = String.format("SELECT * FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = '%s_%s'", schema, name);

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            return rs.next();
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        return null;
    }

    @Override
    public boolean nullsFirst() {
        return false;
    }

    @Override
    public String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields,
                                    List<String> otherFields, String whereForOtherTable) {
        // TODO: COPY PASTE (Oracle)
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
        String result = String.format(template, fieldsStr, otherFieldsStr, otherTableStr, whereForOtherTable);
        return result;
    }


    @Override
    public void createSysObjects(Connection conn, String sysSchemaName) {
        String sql = "CREATE OR ALTER EXCEPTION VERSION_CHECK_ERROR 'record version check failure'";

        try {
            Statement stmt = conn.createStatement();
            try {
                stmt.executeUpdate(sql);
            } finally {
                stmt.close();
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not create or alter versioncheck exception: %s", e.getMessage());
        }
    }

    @Override
    public List<String> getViewList(Connection conn, Grain g) {
        List<String> result = new ArrayList<>();

        String sql = String.format("select rdb$relation_name\n" +
            "from rdb$relations\n" +
            "where rdb$view_blr is not null \n" +
            "and (rdb$system_flag is null or rdb$system_flag = 0)" +
            "and rdb$relation_name like '%s@_%%' escape '@'", g.getName());

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
        return tableElement.getPkConstraintName() + "_" + tableElement.getGrain().getName();
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

    private String getSchemaUnderscoreNameTemplate(String schemaName, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append(stripNameFromQuotes(schemaName)).append("_").append(stripNameFromQuotes(name));

        return sb.toString();
    }

    private String stripNameFromQuotes(String name) {
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
