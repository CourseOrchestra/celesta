package ru.curs.celesta.common;

import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CollatedMSSQLServerContainer<SELF extends CollatedMSSQLServerContainer<SELF>>
        extends MSSQLServerContainer<SELF> {
    public static final String DATABASE_NAME = "celesta";
    private String collation;
    private boolean isCustomDbCreated;

    public SELF withCollation(final String collation) {
        this.collation = collation;
        return self();
    }

    @Override
    public void start() {
        super.start();
        if (this.collation != null) {
            createCustomDataBase();
        }
    }

    private void createCustomDataBase() {
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
        StringBuilder sb = new StringBuilder(
                "jdbc:sqlserver://" + getContainerIpAddress() + ":" + getMappedPort(MS_SQL_SERVER_PORT));
        if (isCustomDbCreated) {
            sb.append(";databaseName=").append(DATABASE_NAME);
        }
        return sb.toString();
    }
}
