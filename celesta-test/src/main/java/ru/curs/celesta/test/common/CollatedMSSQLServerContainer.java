package ru.curs.celesta.test.common;

import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

public class CollatedMSSQLServerContainer<SELF extends CollatedMSSQLServerContainer<SELF>>
        extends MSSQLServerContainer<SELF> {
    public static final String DATABASE_NAME = "celesta";
    private String collation;
    private boolean isCustomDbCreated;

    public CollatedMSSQLServerContainer() {
        super("mcr.microsoft.com/mssql/server:2017-latest");
    }

    public SELF withCollation(final String collation) {
        this.collation = collation;
        return self();
    }

    @Override
    public void start() {
        withLogConsumer(s -> System.out.println(s.getUtf8String()));
        super.start();
        if (this.collation != null) {
            createCustomDataBase();
        }
    }

    @Override
    protected void configure() {
        super.configure();
        this.addEnv("SET_CONTAINER_TIMEZONE", "true");
        this.addEnv("TZ", TimeZone.getDefault().getID());
    }

    private void createCustomDataBase() {
        this.isCustomDbCreated = false;

        try (Connection conn = createConnection("");
             Statement stmt = conn.createStatement()
        ) {
            StringBuilder sqlBuilder = new StringBuilder("CREATE DATABASE ")
                    .append(DATABASE_NAME);
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

    @Override
    public String getJdbcUrl() {
        StringBuilder sb = new StringBuilder(this.getInitJdbcUrl());
        if (this.isCustomDbCreated) {
            sb.append(";databaseName=").append(DATABASE_NAME);
        }
        return sb.toString();
    }

    public String getInitJdbcUrl() {
        return "jdbc:sqlserver://" + getHost() + ":" + getMappedPort(MS_SQL_SERVER_PORT);
    }
}
