package ru.curs.celesta.test;

import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.curs.celesta.test.common.AdvancedFireBirdContainer;
import ru.curs.celesta.test.common.CollatedMSSQLServerContainer;


public final class ContainerUtils {
    /**
     * PostgreSQL container.
     */
    public static final PostgreSQLContainer<?> POSTGRE_SQL = getPostgreSQLContainer();

    /**
     * Oracle container.
     */
    public static final OracleContainer ORACLE = getOracleContainer();

    /**
     * MS SQL Server container.
     */
    public static final CollatedMSSQLServerContainer<?> MSSQL = getMSSQLContainer();

    /**
     * Firebird container.
     */
    public static final AdvancedFireBirdContainer FIREBIRD = getFireBirdContainer();

    private ContainerUtils() {
    }

    public static PostgreSQLContainer<?> getPostgreSQLContainer() {
        return new PostgreSQLContainer<>("postgres:14.0");
    }

    public static OracleContainer getOracleContainer() {
        return new OracleContainer("gvenzl/oracle-xe:21.3.0");
    }

    public static CollatedMSSQLServerContainer<?> getMSSQLContainer() {
        return new CollatedMSSQLServerContainer<>()
                .withCollation("Cyrillic_General_CI_AI");
    }

    public static AdvancedFireBirdContainer getFireBirdContainer() {
        return new AdvancedFireBirdContainer();
    }

}
