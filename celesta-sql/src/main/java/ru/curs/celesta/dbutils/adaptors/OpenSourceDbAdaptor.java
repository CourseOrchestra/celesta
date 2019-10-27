package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.dbutils.query.FromClause;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.TableElement;

/**
 * Created by ioann on 02.05.2017.
 */
public abstract class OpenSourceDbAdaptor extends DBAdaptor {

    protected static final String SELECT_S_FROM = "select %s from ";
    protected static final Pattern DATEPATTERN = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");

    protected static final Pattern QUOTED_NAME = Pattern.compile("\"?([^\"]+)\"?");

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSourceDbAdaptor.class);

    public OpenSourceDbAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
        super(connectionPool, ddlConsumer);
    }

    @Override
    public boolean tableExists(Connection conn, String schema, String name) {
        try (PreparedStatement check = conn
                .prepareStatement(String.format("SELECT table_name FROM information_schema.tables  WHERE "
                                + "table_schema = '%s' AND table_name = '%s'",
                        schema,
                        name
                ));
             ResultSet rs = check.executeQuery()) {
            return rs.next();
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    @Override
    void createSchemaIfNotExists(Connection conn, String name) {
        String sql = String.format(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s';", name
        );

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            if (!rs.next()) {
                ddlAdaptor.createSchema(conn, name);
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public PreparedStatement getOneFieldStatement(Connection conn, Column<?> c, String where) {
        TableElement t = c.getParentTable();
        String sql = String.format(SELECT_S_FROM + tableString(t.getGrain().getName(), t.getName())
                + " where %s limit 1;", c.getQuotedName(), where);
        return prepareStatement(conn, sql);
    }

    @Override
    public PreparedStatement getOneRecordStatement(
            Connection conn, TableElement t, String where, Set<String> fields
    ) {

        final String fieldList = getTableFieldsListExceptBlobs((DataGrainElement) t, fields);
        String sql = String.format(SELECT_S_FROM + tableString(t.getGrain().getName(), t.getName())
                + " where %s limit 1;", fieldList, where);

        PreparedStatement result = prepareStatement(conn, sql);
        LOGGER.trace("{}", result);
        return result;
    }

    @Override
    public PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) {
        String sql = String.format("delete from " + tableString(t.getGrain().getName(), t.getName()) + " where %s;",
                where);
        return prepareStatement(conn, sql);
    }

    @Override
    public Set<String> getColumns(Connection conn, TableElement t) {
        String sql = String.format("select column_name from information_schema.columns "
                        + "where table_schema = '%s' and table_name = '%s';",
                t.getGrain().getName().replace("\"", ""),
                t.getName().replace("\"", ""));
        return sqlToStringSet(conn, sql);
    }

    @Override
    public PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) {
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
        if (orderBy.length() > 0) {
            w.append(" order by " + orderBy);
        }
        String sql = String.format(SELECT_S_FROM + " %s %s  limit 1 offset %d;", fieldList,
                from.getExpression(), useWhere ? " where " + w : w, offset == 0 ? 0 : offset - 1);
        LOGGER.trace(sql);
        return prepareStatement(conn, sql);
    }

    @Override
    public boolean nullsFirst() {
        return false;
    }

    @Override
    public long nextSequenceValue(Connection conn, SequenceElement s) {
        String sql = "SELECT NEXTVAL('" + sequenceString(s.getGrain().getName(), s.getName()) + "')";

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
        try (
                PreparedStatement preparedStatement = conn.prepareStatement(
                        "SELECT * FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ? AND SEQUENCE_NAME = ?"
                )
        ) {
            preparedStatement.setString(1, schema.replace("\"", ""));
            preparedStatement.setString(2, name.replace("\"", ""));
            try (ResultSet rs = preparedStatement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

}
