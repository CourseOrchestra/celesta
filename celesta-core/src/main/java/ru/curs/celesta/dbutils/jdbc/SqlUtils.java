package ru.curs.celesta.dbutils.jdbc;

import ru.curs.celesta.CelestaException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SQL Utility class.
 */
public final class SqlUtils {

    private SqlUtils() {
    }

    /**
     * Executes update statement on DB connection.
     *
     * @param conn DB connection
     * @param sql  SQL update statement
     */
    public static int executeUpdate(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    /**
     * Executes query statement on DB connection, returning resultset as a lambda parameter.
     * <p>
     * This method releases the respective ResultSet and Statement, and also handles exceptions.
     *
     * @param conn   DB connection
     * @param sql    SQL query statement
     * @param action lambda to be executed for ResultSet
     */
    public static void executeQuery(Connection conn, String sql,
                                    SQLAction action) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            action.invoke(rs);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    /**
     * Executes query statement on DB connection, returning resultset as a lambda parameter.
     * <p>
     * This method releases the respective ResultSet and Statement, and also handles exceptions.
     *
     * @param <T>    lambda return type
     * @param conn   DB connection
     * @param sql    SQL query statement
     * @param action lambda to be executed for ResultSet
     * @return Result of lambda execution
     */
    public static <T> T executeQuery(Connection conn, String sql,
                                     SQLActionReturning<T> action) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return action.invoke(rs);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    /**
     * Executes query statement on DB connection, returning resultset as a lambda parameter.
     * <p>
     * This method releases the respective ResultSet and Statement, and also handles exceptions.
     *
     * @param conn   DB connection
     * @param sql    SQL query statement
     * @param errMsg Message to be added to the error
     * @param action lambda to be executed for ResultSet
     */
    public static void executeQuery(Connection conn, String sql,
                                    SQLAction action, String errMsg) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            action.invoke(rs);
        } catch (SQLException e) {
            CelestaException ce = new CelestaException("%s: %s",
                    errMsg,
                    e.toString());
            ce.initCause(e);
            throw ce;
        }
    }

    /**
     * Executes query statement on DB connection, returning resultset as a lambda parameter.
     * <p>
     * This method releases the respective ResultSet and Statement, and also handles exceptions.
     *
     * @param <T>    lambda return type
     * @param conn   DB connection
     * @param sql    SQL query statement
     * @param errMsg Message to be added to the error
     * @param action lambda to be executed for ResultSet
     * @return Result of lambda execution
     */
    public static <T> T executeQuery(Connection conn, String sql,
                                     SQLActionReturning<T> action, String errMsg) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return action.invoke(rs);
        } catch (SQLException e) {
            CelestaException ce = new CelestaException("%s: %s",
                    errMsg,
                    e.toString());
            ce.initCause(e);
            throw ce;
        }
    }
}
