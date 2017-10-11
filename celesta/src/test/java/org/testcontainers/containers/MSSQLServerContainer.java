package org.testcontainers.containers;

import org.testcontainers.containers.wait.HostPortWaitStrategy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class MSSQLServerContainer<SELF extends MSSQLServerContainer<SELF>> extends JdbcDatabaseContainer<SELF>  {
    static final String NAME = "mssqlserver";
    static final String IMAGE = "microsoft/mssql-server-linux";
    public static final Integer MS_SQL_SERVER_PORT = 1433;
    private String username = "SA";
    private String password = "A_Str0ng_Required_Password";
    private String collation = "Cyrillic_General_CI_AS";

    public MSSQLServerContainer() {
        this(IMAGE + ":2017-latest");
    }

    public MSSQLServerContainer(final String dockerImageName) {
        super(dockerImageName);
        this.waitStrategy = new HostPortWaitStrategy()
                .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS));
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(MS_SQL_SERVER_PORT);
    }

    @Override
    protected void configure() {

        addExposedPort(MS_SQL_SERVER_PORT);
        addEnv("ACCEPT_EULA", "Y");
        addEnv("SA_PASSWORD", password);
        addEnv("MSSQL_COLLATION", collation);
        addEnv("TZ", System.getProperty("user.timezone"));
    }

    @Override
    public String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:sqlserver://" + getContainerIpAddress() + ":" + getMappedPort(MS_SQL_SERVER_PORT);
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
