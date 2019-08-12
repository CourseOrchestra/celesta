package ru.curs.celesta.test;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ConnectionPoolConfiguration;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.test.common.CollatedMSSQLServerContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ContainerUtils {

    public static final PostgreSQLContainer POSTGRE_SQL = new PostgreSQLContainer();
    public static final OracleContainer ORACLE = new OracleContainer();
    public static final CollatedMSSQLServerContainer MSSQL = new CollatedMSSQLServerContainer()
        .withCollation("Cyrillic_General_CI_AI");


    private static final String DROP_TABLE_FROM_ORACLE_TEMPLATE = "DROP TABLE %s CASCADE CONSTRAINTS";
    private static final String DROP_OBJECT_FROM_ORACLE_TEMPLATE = "DROP %s %s";

    private static final Map<Class<? extends JdbcDatabaseContainer>, Runnable> CLEAN_UP_MAP = new HashMap<>();

    static {
        CLEAN_UP_MAP.put(POSTGRE_SQL.getClass(), ContainerUtils::cleanUpPostgres);
        CLEAN_UP_MAP.put(ORACLE.getClass(), ContainerUtils::cleanUpOracle);
        CLEAN_UP_MAP.put(MSSQL.getClass(), ContainerUtils::cleauUpMsSql);
    }


    public static void cleanUp(JdbcDatabaseContainer container) {
        CLEAN_UP_MAP.get(container.getClass()).run();
    }

    private static void cleanUpPostgres() {
        ConnectionPool connectionPool = getConnectionPool(POSTGRE_SQL);
        Connection connection = connectionPool.get();

        try (ResultSet rs = SqlUtils.executeQuery(
            connection,
            "select distinct schemaname " +
                "         from pg_catalog.pg_tables " +
                "         " +
                "         where schemaname not in ('pg_catalog', 'information_schema') "
        )) {
            while (rs.next()) {
                SqlUtils.executeUpdate(connection,
                    String.format("DROP SCHEMA %s CASCADE", rs.getString(1)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleanUpOracle() {
        ConnectionPool connectionPool = getConnectionPool(ORACLE);
        Connection connection = connectionPool.get();

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
                "                              'SEQUENCE',\n" +
                "                              'SYNONYM',\n" +
                "                              'PACKAGE BODY'\n" +
                "                             )"
        )) {
            while (rs.next()) {
                String objectType = rs.getString("object_type");
                String objectName = rs.getString("object_name");

                if ("TABLE".equalsIgnoreCase(objectType)) {
                    SqlUtils.executeUpdate(
                        connection,
                        String.format(DROP_TABLE_FROM_ORACLE_TEMPLATE, objectName)
                    );
                } else {
                    try {
                        SqlUtils.executeUpdate(
                            connection,
                            String.format(DROP_OBJECT_FROM_ORACLE_TEMPLATE, objectType, objectName)
                        );
                    } catch (CelestaException e) {
                        SQLException sqlException = (SQLException) e.getCause();

                        // Object not found -> cascaded deleted with table
                        if ( sqlException.getErrorCode() == 942) {
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleauUpMsSql() {
        ConnectionPool connectionPool = getConnectionPool(MSSQL);
        Connection connection = connectionPool.get();

        try (ResultSet rs = SqlUtils.executeQuery(connection, "" +
            "SELECT s.SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA s\n" +
            "WHERE s.SCHEMA_NAME NOT IN('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')")) {
            while (rs.next()) {
                SqlUtils.executeUpdate(connection,
                    String.format("DROP SCHEMA %s", rs.getString(1)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectionPool getConnectionPool(JdbcDatabaseContainer container) {
        ConnectionPoolConfiguration poolConfiguration = new ConnectionPoolConfiguration();
        poolConfiguration.setJdbcConnectionUrl(container.getJdbcUrl());
        poolConfiguration.setLogin(container.getUsername());
        poolConfiguration.setPassword(container.getPassword());
        poolConfiguration.setDriverClassName(container.getDriverClassName());

        return ConnectionPool.create(poolConfiguration);
    }
}
