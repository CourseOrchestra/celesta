package org.testcontainers.containers;

import org.testcontainers.containers.wait.HostPortWaitStrategy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class MSSQLServerContainer<SELF extends MSSQLServerContainer<SELF>> extends JdbcDatabaseContainer<SELF>  {
    static final String NAME = "mssqlserver";
    static final String IMAGE = "microsoft/mssql-server-linux";
    public static final Integer MS_SQL_SERVER_PORT = 1433;
    private String username = "SA";
    private String password = "A_Str0ng_Required_Password";
    private String collation;
    private String databaseName;
    private boolean isCustomDbCreated;

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
        //addEnv("MSSQL_COLLATION", collation);
        addEnv("TZ", System.getProperty("user.timezone"));
    }

    @Override
    public String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getJdbcUrl() {
        StringBuilder sb = new StringBuilder("jdbc:sqlserver://" + getContainerIpAddress() + ":" + getMappedPort(MS_SQL_SERVER_PORT));

        if (isCustomDbCreated) {
            sb.append(";databaseName=").append(databaseName);
        }

        return sb.toString();
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

    @Override
    public SELF withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return self();
    }

    @Override
    public void start() {
        super.start();

        if (this.databaseName != null) {
            createCustomDataBase();
        }
    }

    public SELF withCollation(final String collation) {
        this.collation = collation;
        return self();
    }

    private void createCustomDataBase() {
        try (Connection conn = createConnection("");
             Statement stmt = conn.createStatement()
        ) {
            StringBuilder sqlBuilder = new StringBuilder("CREATE DATABASE ")
                    .append(databaseName);

            if (this.collation != null) {
                sqlBuilder.append(" COLLATE " + collation);
            }

            stmt.executeUpdate(sqlBuilder.toString());
            conn.commit();
            isCustomDbCreated = true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
