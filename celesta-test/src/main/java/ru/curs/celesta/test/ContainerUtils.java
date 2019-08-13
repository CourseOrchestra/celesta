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
import java.util.*;


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
        CLEAN_UP_MAP.put(MSSQL.getClass(), ContainerUtils::cleanUpMsSql);
    }

    public static void cleanUp(JdbcDatabaseContainer container) {
        CLEAN_UP_MAP.get(container.getClass()).run();
    }

    private static void cleanUpPostgres() {
        try (ConnectionPool connectionPool = getConnectionPool(POSTGRE_SQL)) {
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
                        String.format("DROP SCHEMA IF EXISTS %s CASCADE", rs.getString(1)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void cleanUpOracle() {
        try (ConnectionPool connectionPool = getConnectionPool(ORACLE)) {
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
                                String.format(DROP_TABLE_FROM_ORACLE_TEMPLATE, objectName)
                            );
                        } else {

                            SqlUtils.executeUpdate(
                                connection,
                                String.format(DROP_OBJECT_FROM_ORACLE_TEMPLATE, objectType, objectName)
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void cleanUpMsSql() {
        try (ConnectionPool connectionPool = getConnectionPool(MSSQL)) {
            Connection connection = connectionPool.get();

            try (ResultSet rs = SqlUtils.executeQuery(connection, "" +
                "SELECT s.SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA s\n" +
                "WHERE s.SCHEMA_NAME NOT IN('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')")) {

                while (rs.next()) {
                    String schemaName = rs.getString(1);
                    List<String> dropStatements = new ArrayList<>();
                    try (ResultSet rs2 = SqlUtils.executeQuery(connection,
                        String.format("SELECT 'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id))\n" +
                            "         +'.'+ QUOTENAME(OBJECT_NAME(s.parent_object_id))\n" +
                            "         + ' DROP CONSTRAINT ' +  QUOTENAME(s.name)\n" +
                            "FROM sys.foreign_keys s\n" +
                            "       JOIN sys.tables t on s.parent_object_id = t.object_id\n" +
                            "       JOIN sys.tables t2 on s.referenced_object_id = t2.object_id\n" +
                            "WHERE t2.schema_id = SCHEMA_ID('%s')", schemaName))) {
                        while (rs2.next()) {
                            dropStatements.add(rs2.getString(1));
                        }
                    }

                    try (ResultSet rs3 = SqlUtils.executeQuery(connection,
                        String.format("SELECT 'DROP ' +\n" +
                            "       CASE WHEN type IN ('P','PC') THEN 'PROCEDURE'\n" +
                            "            WHEN type =  'U' THEN 'TABLE'\n" +
                            "            WHEN type IN ('IF','TF','FN') THEN 'FUNCTION'\n" +
                            "            WHEN type = 'V' THEN 'VIEW'\n" +
                            "            WHEN type = 'SO' THEN 'SEQUENCE'\n" +
                            "           END +\n" +
                            "       ' ' +  QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name)\n" +
                            "FROM sys.objects\n" +
                            "WHERE schema_id = SCHEMA_ID('%s')\n" +
                            "  AND type IN('P','PC','U','IF','TF','FN','V', 'SO')\n" +
                            "ORDER BY  CASE WHEN type IN ('P','PC') THEN 4\n" +
                            "               WHEN type =  'U' THEN 3\n" +
                            "               WHEN type IN ('IF','TF','FN') THEN 1\n" +
                            "               WHEN type = 'V' THEN 2\n" +
                            "               WHEN type = 'SO' THEN 5\n" +
                            "    END", schemaName))) {
                        while (rs3.next()) {
                            dropStatements.add(rs3.getString(1));
                        }
                    }

                    try (ResultSet rs4 = SqlUtils.executeQuery(connection, String.format("SELECT 'DROP XML SCHEMA COLLECTION' \n" +
                        "  + QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name)\n" +
                        "  FROM sys.xml_schema_collections\n" +
                        "  WHERE schema_id = SCHEMA_ID('%s')", schemaName))) {
                        if ( rs4.next()) {
                            dropStatements.add(rs4.getString(1));
                        }
                    }

                    dropStatements.forEach(sql -> SqlUtils.executeUpdate(connection, sql));

                    SqlUtils.executeUpdate(connection, String.format("DROP SCHEMA %s", schemaName));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try (Connection containerConnection = MSSQL.createConnection("")) {
            SqlUtils.executeUpdate(
                containerConnection,
                String.format(
                    "DROP DATABASE IF EXISTS %s",
                    CollatedMSSQLServerContainer.DATABASE_NAME
                )
            );
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
