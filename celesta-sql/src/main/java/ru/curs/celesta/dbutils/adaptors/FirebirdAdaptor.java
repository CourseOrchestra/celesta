package ru.curs.celesta.dbutils.adaptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlGenerator;
import ru.curs.celesta.dbutils.adaptors.ddl.FirebirdDdlGenerator;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FirebirdAdaptor extends DBAdaptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebirdAdaptor.class);


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
        return null;
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
        return null;
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
        return null;
    }

    @Override
    public Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) {
        return null;
    }

    @Override
    public List<String> getParameterizedViewList(Connection conn, Grain g) {
        return null;
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



    // TODO:: Start of copy-pasting from OraAdaptor
    @Override
    public String tableString(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder(getSchemaUnderscoreNameTemplate(schemaName, tableName));
        sb.insert(0, '"').append('"');

        return sb.toString();
    }

    private String getSchemaUnderscoreNameTemplate(String schemaName, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append(stripNameFromQuotes(schemaName)).append("_").append(stripNameFromQuotes(name));

        return sb.toString();
    }

    private String stripNameFromQuotes(String name) {
        return name.startsWith("\"") ? name.substring(1, name.length() - 1) : name;
    }
    // TODO:: End of copy-pasting from OraAdaptor
}
