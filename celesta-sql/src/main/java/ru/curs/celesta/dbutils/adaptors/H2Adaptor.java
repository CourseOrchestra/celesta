package ru.curs.celesta.dbutils.adaptors;

import org.h2.value.DataType;
import ru.curs.celesta.DBType;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;

import static ru.curs.celesta.dbutils.adaptors.constants.OpenSourceConstants.*;

import ru.curs.celesta.dbutils.adaptors.ddl.*;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Created by ioann on 02.05.2017.
 */
final public class H2Adaptor extends OpenSourceDbAdaptor {

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
                      + "WHERE table_type = 'TABLE' AND table_schema <> 'INFORMATION_SCHEMA';");
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
                     + "values (%s)", fields.toString(), params.toString()
        );

        return prepareStatement(conn, sql);
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        String sql = String.format(
                "SELECT ALIAS_NAME FROM INFORMATION_SCHEMA.FUNCTION_ALIASES where alias_schema = '%s'",
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


                    String columnDefaultForIdentity = "NEXTVAL('" + tableString(grainName, tableName + "_seq") + "')";

                    if ("integer".equalsIgnoreCase(typeName)
                            && columnDefaultForIdentity.equals(columnDefault)) {
                        result.setType(IntegerColumn.class);
                        result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                        return result;
                    } else if ("clob".equalsIgnoreCase(typeName)) {
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
            throw new CelestaException(e.getMessage());
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
            if (NOW.equalsIgnoreCase(defaultBody)) {
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
            if (defaultBody.contains("STRINGDECODE")) {
                //H2 отдает default для срок в виде функции, которую нужно выполнить отдельным запросом
                String sql = "SELECT " + defaultBody;

                try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
                    if (rs.next()) {
                        //H2 не сохраняет кавычки в default, если используется не Unicode
                        result = "'" + rs.getString(1) + "'";
                    } else {
                        throw new CelestaException("Can't decode default '" + defaultBody + "'");
                    }
                } catch (SQLException e) {
                    throw new CelestaException("Can't modify default for '" + defaultBody + "'", e);
                }

            }
        }

        return result;
    }


    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        String sql = String.format(
                "SELECT constraint_name AS indexName, column_name as colName "
              + "FROM  INFORMATION_SCHEMA.INDEXES "
              + "WHERE table_schema = '%s' "
                      + "AND table_name = '%s' "
                      + "AND index_type_name = 'PRIMARY KEY'",
                t.getGrain().getName(), t.getName());
        DbPkInfo result = new DbPkInfo(this);

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
    public List<DbFkInfo> getFKInfo(Connection conn, Grain g) {

        String sql = "SELECT FK_NAME AS FK_CONSTRAINT_NAME, "
                          + "FKTABLE_NAME AS FK_TABLE_NAME, "
                          + "FKCOLUMN_NAME AS FK_COLUMN_NAME, "
                          + "PKTABLE_SCHEMA AS REF_GRAIN, "
                          + "PKTABLE_NAME AS REF_TABLE_NAME, "
                          + "UPDATE_RULE, "
                          + "DELETE_RULE "
                   + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                   + "WHERE FKTABLE_SCHEMA = '%s' "
                   + "ORDER BY FK_CONSTRAINT_NAME, ORDINAL_POSITION";
        sql = String.format(sql, g.getName());

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
                    + String.format(" limit -1 offset %d", offset);
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
                "SELECT table_name as tableName, index_name as indexName, column_name as colName "
              + "FROM INFORMATION_SCHEMA.INDEXES "
              + "WHERE table_schema = '%s' AND primary_key <> true",
              g.getName());

        try {
            Statement stmt = conn.createStatement();

            try {
                ResultSet rs = stmt.executeQuery(sql);

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
        String sql = String.format("select count(*) from information_schema.triggers where "
                        + "        table_schema = '%s' and table_name = '%s'"
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
        int dataType = DataType.getTypeFromClass(value.getClass());
        DataType type = DataType.getDataType(dataType);
        return "CAST(? as " + type.name + ") as " + colName;
    }

    @Override
    public DBType getType() {
        return DBType.H2;
    }

    @Override
    public DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) {
        String sql = "SELECT INCREMENT, MIN_VALUE, MAX_VALUE, IS_CYCLE "
                   + "FROM INFORMATION_SCHEMA.SEQUENCES "
                   + "WHERE SEQUENCE_SCHEMA = ? AND SEQUENCE_NAME = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, s.getGrain().getName());
            preparedStatement.setString(2, s.getName());

            try (ResultSet rs = preparedStatement.executeQuery()) {
                rs.next();
                DbSequenceInfo result = new DbSequenceInfo();
                result.setIncrementBy(rs.getLong("INCREMENT"));
                result.setMinValue(rs.getLong("MIN_VALUE"));
                result.setMaxValue(rs.getLong("MAX_VALUE"));
                result.setCycle(rs.getBoolean("IS_CYCLE"));
                return result;
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }
}
