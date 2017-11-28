package org.testcontainers.containers;

public class MSSQLServerContainerProvider extends JdbcDatabaseContainerProvider {
    @Override
    public boolean supports(String databaseType) {
        return databaseType.equals(MSSQLServerContainer.NAME);
    }

    @Override
    public JdbcDatabaseContainer newInstance(String tag) {
        return new MSSQLServerContainer(MSSQLServerContainer.IMAGE + ":" + tag);
    }
}
