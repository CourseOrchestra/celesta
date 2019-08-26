package ru.curs.celesta.dbutils.adaptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
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

public class FirebirdAdaptor extends DBAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebirdAdaptor.class);

    private static final Pattern TABLE_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");


    public FirebirdAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    DdlGenerator getDdlGenerator() {
        return new FirebirdDdlGenerator(this);
    }

    @Override
    String getLimitedSQL(FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields) {
        return null;
    }

    @Override
    String getSelectTriggerBodySql(TriggerQuery query) {
        return null;
    }

    @Override
    boolean userTablesExist(Connection conn) throws SQLException {
        return false;
    }

    @Override
    void createSchemaIfNotExists(Connection conn, String name) {

    }

    @Override
    public PreparedStatement getNavigationStatement(Connection conn, FromClause from, String orderBy, String navigationWhereClause, Set<String> fields, long offset) {
        return null;
    }

    @Override
    public boolean tableExists(Connection conn, String schema, String name) {
        return false;
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
        return null;
    }

    @Override
    public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) {
        return null;
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
        return 0;
    }

    @Override
    public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) {
        return null;
    }

    @Override
    public DbColumnInfo getColumnInfo(Connection conn, Column c) {
        return null;
    }

    @Override
    public DbPkInfo getPKInfo(Connection conn, TableElement t) {
        return null;
    }

    @Override
    public List<DbFkInfo> getFKInfo(Connection conn, Grain g) {
        String sql = String.format(
            "select relc.RDB$RELATION_NAME as table_name, relc.RDB$CONSTRAINT_NAME as constraint_name, " +
                "refc.RDB$UPDATE_RULE as update_rule, refc.RDB$DELETE_RULE as delete_rule, " +
                "d1.RDB$FIELD_NAME as column_name, d2.RDB$DEPENDED_ON_NAME AS ref_table_name \n" +
                "FROM RDB$RELATION_CONSTRAINTS relc \n" +
                "LEFT JOIN RDB$REF_CONSTRAINTS refc ON relc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME \n" +
                "LEFT JOIN RDB$DEPENDENCIES d1 ON d1.RDB$DEPENDED_ON_NAME = relc.RDB$RELATION_NAME \n" +
                "LEFT JOIN RDB$DEPENDENCIES d2 ON d1.RDB$DEPENDENT_NAME = d2.RDB$DEPENDENT_NAME \n" +
                "WHERE relc.RDB$CONSTRAINT_TYPE <> 'FOREIGN KEY' AND relc.RDB$RELATION_NAME like '%s@_%%' escape '@' " +
                "AND d1.RDB$DEPENDED_ON_NAME <> d2.RDB$DEPENDED_ON_NAME " +
                "AND d1.RDB$FIELD_NAME <> d2.RDB$FIELD_NAME",
            g.getName()
        );


        Map<String, DbFkInfo> fks = new HashMap<>();

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                String fkName = rs.getString("constraint_name");

                String fullTableName = rs.getString("table_name").trim();
                String tableName = convertNameFromDb(fullTableName, g);

                String fullRefTableName = rs.getString("ref_table_name").trim();
                String refTableName = convertNameFromDb(fullRefTableName, g);
                String refGrainName = fullRefTableName.substring(0, fullRefTableName.indexOf(refTableName) - 1);

                FKRule updateRule = getFKRule(rs.getString("update_rule"));
                FKRule deleteRule = getFKRule(rs.getString("delete_rule"));

                String columnName = rs.getString("column_name");

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
            "select ind.RDB$RELATION_NAME as tablename, ind.RDB$INDEX_NAME as indexname, \n" +
            "inds.RDB$FIELD_NAME as columnname \n" +
            "FROM RDB$INDICES ind \n" +
            "INNER JOIN RDB$INDEX_SEGMENTS inds \n" +
            "ON ind.RDB$INDEX_NAME = inds.RDB$INDEX_NAME \n" +
            "LEFT JOIN RDB$RELATION_CONSTRAINTS rc \n" +
            "ON ind.RDB$INDEX_NAME = rc.RDB$INDEX_NAME \n" +
            "WHERE rc.RDB$CONSTRAINT_TYPE <> 'PRIMARY KEY' AND ind.RDB$RELATION_NAME like '%s@_%%' escape '@'",
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
                i.getColumnNames().add(rs.getString("columnname"));

            }
        } catch (Exception e) {
            throw new CelestaException(e);
        }

        return result;
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        List<String> result = new ArrayList<>();

        String sql = String.format("select rdb$function_name\n" +
            "from rdb$functions\n" +
            "where rdb$function_name like '%s@_%%' escape '@'", g.getName());

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new CelestaException(e);
        }

        return result;
    }

    @Override
    public int getDBPid(Connection conn) {
        return 0;
    }

    @Override
    public DBType getType() {
        return null;
    }

    @Override
    public long nextSequenceValue(Connection conn, SequenceElement s) {
        return 0;
    }

    @Override
    public boolean sequenceExists(Connection conn, String schema, String name) {
        return false;
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
    public String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields, List<String> otherFields, String whereForOtherTable) {
        return null;
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
                result.add(rs.getString(1));
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

    // TODO:: End of copy-pasting from OraAdaptor
}
