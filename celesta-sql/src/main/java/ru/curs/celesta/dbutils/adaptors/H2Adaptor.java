package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlGenerator;
import ru.curs.celesta.dbutils.adaptors.ddl.H2DdlGenerator;
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
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.TableElement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by ioann on 02.05.2017.
 */
public final class H2Adaptor extends OpenSourceDbAdaptor {
    private static final Pattern HEX_STRING = Pattern.compile("X'([0-9A-Fa-f]+)'");


    public H2Adaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer, boolean isH2ReferentialIntegrity) {
        super(connectionPool, ddlConsumer);
        configureDb(isH2ReferentialIntegrity);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new H2DdlGenerator(this);
    }

    private void configureDb(boolean isH2ReferentialIntegrity) {

        try (Connection connection = connectionPool.get()) {
            //Выполняем команду включения флага REFERENTIAL_INTEGRITY
            String sql = "SET REFERENTIAL_INTEGRITY " + isH2ReferentialIntegrity;

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't manage REFERENTIAL_INTEGRITY", e);
        }
    }

    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        try (
                PreparedStatement check = conn.prepareStatement(
                        "SELECT COUNT(*) FROM information_schema.tables "
                                + "WHERE table_type = 'BASE TABLE' AND table_schema <> 'INFORMATION_SCHEMA';");
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

        String sql = String.format("select CURRVAL('\"%s\".\"%s\"')", t.getGrain().getName(), sequenceName);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }


    @Override
    public PreparedStatement getInsertRecordStatement(
            Connection conn, BasicTable t, boolean[] nullsMask, List<ParameterSetter> program) {

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


        String sql = String.format(
                "insert into " + tableString(t.getGrain().getName(), t.getName()) + " (%s) "
                        + "values (%s)", fields, params
        );

        return prepareStatement(conn, sql);
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        String sql = String.format(
                "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA = '%s'",
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
    public DbColumnInfo getColumnInfo(Connection conn, Column<?> c) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            String grainName = c.getParentTable().getGrain().getName();
            String tableName = c.getParentTable().getName();

            try (ResultSet rs = metaData.getColumns(null, grainName, tableName, c.getName())) {
                if (rs.next()) {
                    DbColumnInfo result = new DbColumnInfo();
                    result.setName(rs.getString(COLUMN_NAME));
                    String typeName = rs.getString("TYPE_NAME");
                    String columnDefault = rs.getString("COLUMN_DEF");

                    if ("character large object".equalsIgnoreCase(typeName)) {
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

                    if (columnDefault != null) {
                        columnDefault = modifyDefault(result, columnDefault, conn);
                        result.setDefaultValue(columnDefault);
                    }
                    return result;
                } else {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

    private String modifyDefault(DbColumnInfo ci, String defaultBody, Connection conn) {
        String result = defaultBody;
        if (IntegerColumn.class == ci.getType()) {
            Pattern p = Pattern.compile("NEXT VALUE FOR \"[^\"]+\"\\.\"([^\"]+)+\"");
            Matcher m = p.matcher(defaultBody);
            if (m.find()) {
                String sequenceName = m.group(1);
                result = "NEXTVAL(" + sequenceName + ")";
            }
        } else if (DateTimeColumn.class == ci.getType()) {
            if ("localtimestamp".equalsIgnoreCase(defaultBody)) {
                result = "GETDATE()";
            } else {
                Matcher m = DATEPATTERN.matcher(defaultBody);
                m.find();
                result = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
            }
        } else if (BooleanColumn.class == ci.getType()) {
            result = "'" + defaultBody.toUpperCase() + "'";
        } else if (BinaryColumn.class == ci.getType()) {
            Matcher m = HEX_STRING.matcher(defaultBody);
            if (m.find()) {
                result = "0x" + m.group(1).toUpperCase();
            }
        } else if (StringColumn.class == ci.getType()) {
            if (defaultBody.startsWith("U&'")) {
                //H2 отдает default для срок в виде функции, которую нужно выполнить отдельным запросом
                String sql = "SELECT " + defaultBody;

                result = SqlUtils.executeQuery(conn, sql, rs -> {
                    if (rs.next()) {
                        //H2 не сохраняет кавычки в default, если используется не Unicode
                        return "'" + rs.getString(1) + "'";
                    } else {
                        throw new CelestaException("Can't decode default '" + defaultBody + "'");
                    }
                }, String.format("Can't modify default for '%s'", defaultBody));
            }
        }
        return result;
    }


    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        String sql = String.format(
                "SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME%n"
                        + "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc%n"
                        + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu%n"
                        + "ON kcu.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG%n"
                        + "AND kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA%n"
                        + "AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME%n"
                        + "WHERE %n"
                        + "tc.CONSTRAINT_TYPE = 'PRIMARY KEY'%n"
                        + "AND tc.TABLE_SCHEMA = '%s'%n"
                        + "AND tc.TABLE_NAME = '%s'"
                        + "ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION%n",
                t.getGrain().getName(), t.getName());
        DbPkInfo result = new DbPkInfo(this);

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                if (result.getName() == null) {
                    String indName = rs.getString("CONSTRAINT_NAME");
                    result.setName(indName);
                }

                String colName = rs.getString("COLUMN_NAME");
                result.getColumnNames().add(colName);
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not get indices information: %s", e.getMessage());
        }
        return result;
    }

    @Override
    public List<DbFkInfo> getFKInfo(Connection conn, Grain g) {

        String sql = "select %n"
                + "  tc.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, %n"
                + "  tc.TABLE_NAME AS FK_TABLE_NAME,%n"
                + "  kcu.COLUMN_NAME AS FK_COLUMN_NAME, %n"
                + "  rtc.TABLE_SCHEMA as REF_GRAIN,%n"
                + "  rtc.TABLE_NAME as REF_TABLE_NAME,%n"
                + "  rc.UPDATE_RULE, %n"
                + "  rc.DELETE_RULE %n"
                + "from  INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc%n"
                + "INNER JOIN  INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc%n"
                + "ON rc.CONSTRAINT_CATALOG= tc.CONSTRAINT_CATALOG%n"
                + "AND rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA%n"
                + "AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME%n"
                + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu%n"
                + "ON tc.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG %n"
                + "AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA%n"
                + "AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME%n"
                + "AND tc.TABLE_NAME = kcu.TABLE_NAME%n"
                + "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS rtc%n"
                + "ON rc.UNIQUE_CONSTRAINT_CATALOG = rtc.CONSTRAINT_CATALOG%n"
                + "AND rc.UNIQUE_CONSTRAINT_SCHEMA = rtc.CONSTRAINT_SCHEMA%n"
                + "AND rc.UNIQUE_CONSTRAINT_NAME = rtc.CONSTRAINT_NAME%n"
                + "WHERE tc.CONSTRAINT_TYPE='FOREIGN KEY' AND tc.constraint_schema ='%s'"
                + "ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION";
        sql = String.format(sql, g.getName());

        List<DbFkInfo> result = new LinkedList<>();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            DbFkInfo i = null;
            while (rs.next()) {
                String fkName = rs.getString("FK_CONSTRAINT_NAME");
                if (i == null || !i.getName().equals(fkName)) {
                    i = new DbFkInfo(fkName);
                    result.add(i);
                    i.setTableName(rs.getString("FK_TABLE_NAME"));
                    i.setRefGrainName(rs.getString("REF_GRAIN"));
                    i.setRefTableName(rs.getString("REF_TABLE_NAME"));

                    String updateRule = rs.getString("UPDATE_RULE");
                    i.setUpdateRule(getFKRule(updateRule));
                    String deleteRule = rs.getString("DELETE_RULE");
                    i.setDeleteRule(getFKRule(deleteRule));
                }
                i.getColumnNames().add(rs.getString("FK_COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
        return result;
    }

    @Override
    public String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields,
                                    List<String> otherFields, String otherWhere) {
        String template = "( %s ) IN (SELECT %s FROM %s WHERE %s)";
        String fieldsStr = fields.stream()
                .map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(","));
        String otherFieldsStr = otherFields.stream()
                .map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(","));

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
                    + String.format(" offset %d", offset);
        } else {
            sql = getSelectFromOrderBy(from, whereClause, orderBy, fields)
                    + String.format(" limit %d offset %d", rowCount, offset);
        }
        return sql;
    }

    @Override
    public Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) {
        Map<String, DbIndexInfo> result = new HashMap<>();

        String sql = String.format(
                "SELECT i.TABLE_NAME as tableName, ic.INDEX_NAME AS indexName, ic.column_name as colName%n"
                        + "FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic  INNER JOIN INFORMATION_SCHEMA.INDEXES i%n"
                        + "ON %n"
                        + "  ic.INDEX_CATALOG = i.INDEX_CATALOG%n"
                        + "  and ic.INDEX_SCHEMA = i.INDEX_SCHEMA %n"
                        + "  and ic.INDEX_NAME = i.INDEX_NAME%n"
                        + "WHERE i.table_schema = '%s' "
                        + "and i.index_type_name <> 'PRIMARY KEY'%n"
                        + "ORDER BY ic.ordinal_position",
                g.getName());


        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String indexName = rs.getString("indexName");
                DbIndexInfo ii = result.get(indexName);

                if (ii == null) {
                    String tableName = rs.getString("tableName");
                    ii = new DbIndexInfo(tableName, indexName);
                    result.put(indexName, ii);
                }

                String colName = rs.getString("colName");
                ii.getColumnNames().add(colName);
            }
        } catch (SQLException e) {
            throw new CelestaException("Could not get indices information: %s", e.getMessage());
        }

        return result;
    }

    @Override
    public boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException {
        String sql = String.format("select count(*) from information_schema.triggers where "
                        + "        event_object_schema = '%s' and event_object_table = '%s'"
                        + "        and trigger_name = '%s'",
                query.getSchema().replace("\"", ""),
                query.getTableName().replace("\"", ""),
                query.getName());

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            boolean result = rs.getInt(1) > 0;
            return result;
        }
    }


    @Override
    public int getDBPid(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT SESSION_ID()")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            //do nothing
        }
        return 0;
    }

    @Override
    public String translateDate(String date) {
        try {
            Date d = DateTimeColumn.parseISODate(date);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            return String.format("date '%s'", df.format(d));
        } catch (ParseException e) {
            throw new CelestaException(e.getMessage());
        }

    }

    @Override
    String getSelectTriggerBodySql(TriggerQuery query) {
        String sql = String.format("select SQL from information_schema.triggers where "
                + "        table_schema = '%s' and table_name = '%s'"
                + "        and trigger_name = '%s'", query.getSchema(), query.getTableName(), query.getName());

        return sql;
    }

    @Override
    String prepareRowColumnForSelectStaticStrings(String value, String colName, int maxStringLength) {
        return String.format("CAST(? as varchar(%d)) as %s", maxStringLength, colName);
    }

    @Override
    public DBType getType() {
        return DBType.H2;
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        String sql = "SELECT INCREMENT, MINIMUM_VALUE, MAXIMUM_VALUE, CYCLE_OPTION "
                + "FROM INFORMATION_SCHEMA.SEQUENCES "
                + "WHERE SEQUENCE_SCHEMA = ? AND SEQUENCE_NAME = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, s.getGrain().getName());
            preparedStatement.setString(2, s.getName());

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
