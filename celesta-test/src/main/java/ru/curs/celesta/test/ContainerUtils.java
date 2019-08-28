package ru.curs.celesta.test;

import org.firebirdsql.testcontainers.FirebirdContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ConnectionPoolConfiguration;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.test.common.AdvancedFireBirdContainer;
import ru.curs.celesta.test.common.CollatedMSSQLServerContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public class ContainerUtils {

    public static final PostgreSQLContainer POSTGRE_SQL = new PostgreSQLContainer();
    public static final OracleContainer ORACLE = new OracleContainer();
    public static final CollatedMSSQLServerContainer MSSQL = new CollatedMSSQLServerContainer()
        .withCollation("Cyrillic_General_CI_AI");
    public static final FirebirdContainer FIREBIRD = new AdvancedFireBirdContainer();
    // cd /usr/local/firebird/bin
    // echo "CREATE DATABASE '/firebird/data/celesta-test.fdb' user 'a' password 'b';" > init.sql
    // ./isql -input init.sql


    private static final String DROP_TABLE_FROM_ORACLE_TEMPLATE = "DROP TABLE %s CASCADE CONSTRAINTS";
    private static final String DROP_OBJECT_FROM_ORACLE_TEMPLATE = "DROP %s %s";
    private static final String DROP_TYPE_FROM_ORACLE_TEMPLATE = "DROP TYPE %s FORCE";

    private static final Map<Class<? extends JdbcDatabaseContainer>, Runnable> CLEAN_UP_MAP = new HashMap<>();

    static {
        CLEAN_UP_MAP.put(POSTGRE_SQL.getClass(), ContainerUtils::cleanUpPostgres);
        CLEAN_UP_MAP.put(ORACLE.getClass(), ContainerUtils::cleanUpOracle);
        CLEAN_UP_MAP.put(MSSQL.getClass(), ContainerUtils::cleanUpMsSql);
        CLEAN_UP_MAP.put(FIREBIRD.getClass(), ContainerUtils::cleanUpFirebird);
    }

    public static void cleanUp(JdbcDatabaseContainer container) {
        CLEAN_UP_MAP.get(container.getClass()).run();
    }

    private static void cleanUpPostgres() {
        try (
            ConnectionPool connectionPool = getConnectionPool(POSTGRE_SQL);
            Connection connection = connectionPool.get()
        ) {

            try (ResultSet rs = SqlUtils.executeQuery(
                connection,
                "select distinct schemaname " +
                    "         from pg_catalog.pg_tables " +
                    "         " +
                    "         where schemaname not in ('pg_catalog', 'information_schema') "
            )) {
                while (rs.next()) {
                    SqlUtils.executeUpdate(connection,
                        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", rs.getString(1)));
                }
            }

            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleanUpOracle() {
        try (
            ConnectionPool connectionPool = getConnectionPool(ORACLE);
            Connection connection = connectionPool.get()
        ) {

            try (ResultSet rs = SqlUtils.executeQuery(
                connection,
                "SELECT object_name, object_type\n" +
                    "                     FROM user_objects\n" +
                    "                    WHERE object_type IN\n" +
                    "                             ('TABLE',\n" +
                    "                              'VIEW',\n" +
                    "                              'MATERIALIZED VIEW',\n" +
                    "                              'PACKAGE',\n" +
                    "                              'PROCEDURE',\n" +
                    "                              'FUNCTION',\n" +
                    "                              'TYPE',\n" +
                    "                              'SEQUENCE',\n" +
                    "                              'SYNONYM',\n" +
                    "                              'PACKAGE BODY'\n" +
                    "                             )" +
                    "                    AND created > sysdate - interval '1' hour"
            )) {
                while (rs.next()) {
                    try {
                        String objectType = rs.getString("object_type");
                        String objectName = rs.getString("object_name");

                        if ("TABLE".equalsIgnoreCase(objectType)) {
                            SqlUtils.executeUpdate(
                                connection,
                                String.format(DROP_TABLE_FROM_ORACLE_TEMPLATE, String.format("\"%s\"", objectName))
                            );
                        } else if ("TYPE".equalsIgnoreCase(objectType)) {
                            SqlUtils.executeUpdate(
                                connection,
                                String.format(DROP_TYPE_FROM_ORACLE_TEMPLATE, String.format("\"%s\"", objectName))
                            );
                        } else {

                            SqlUtils.executeUpdate(
                                connection,
                                String.format(DROP_OBJECT_FROM_ORACLE_TEMPLATE, objectType, String.format("\"%s\"", objectName))
                            );

                        }
                    } catch (CelestaException e) {
                        SQLException sqlException = (SQLException) e.getCause();

                        // Object not found -> cascaded deleted with table
                        if (Arrays.asList(942, 2289, 4043).contains(sqlException.getErrorCode())) {
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }
            }

            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleanUpMsSql() {

        ConnectionPoolConfiguration connectionPoolConfiguration = connectionPoolConfiguration(MSSQL);
        connectionPoolConfiguration.setJdbcConnectionUrl(MSSQL.getInitJdbcUrl());

        try (
            ConnectionPool connectionPool = ConnectionPool.create(connectionPoolConfiguration);
            Connection connection = connectionPool.get()
        ) {
            SqlUtils.executeUpdate(
                connection,
                String.format(
                    "DROP DATABASE IF EXISTS %s",
                    CollatedMSSQLServerContainer.DATABASE_NAME
                )
            );

            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleanUpFirebird() {
        try (
            ConnectionPool connectionPool = getConnectionPool(FIREBIRD);
            Connection connection = connectionPool.get()
        ) {
            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectionPool getConnectionPool(JdbcDatabaseContainer container) {
        ConnectionPoolConfiguration poolConfiguration = connectionPoolConfiguration(container);
        return ConnectionPool.create(poolConfiguration);
    }

    private static ConnectionPoolConfiguration connectionPoolConfiguration(JdbcDatabaseContainer container) {
        ConnectionPoolConfiguration poolConfiguration = new ConnectionPoolConfiguration();
        poolConfiguration.setJdbcConnectionUrl(container.getJdbcUrl());
        poolConfiguration.setLogin(container.getUsername());
        poolConfiguration.setPassword(container.getPassword());
        poolConfiguration.setDriverClassName(container.getDriverClassName());

        return poolConfiguration;
    }
}
