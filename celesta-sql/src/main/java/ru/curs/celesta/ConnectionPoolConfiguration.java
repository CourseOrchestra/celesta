package ru.curs.celesta;

/**
 * Configuration for {@link ConnectionPool}.
 */
public class ConnectionPoolConfiguration {

    private String jdbcConnectionUrl;
    private String driverClassName;
    private String login;
    private String password;

    /**
     * Returns {@link String} jdbc url of db connection.
     *
     * @return jdbc url of db connection
     */
    public String getJdbcConnectionUrl() {
        return jdbcConnectionUrl;
    }

    /**
     * Sets {@link String} jdbc url of db connection.
     * @param jdbcConnectionUrl jdbc url of db connection
     */
    public void setJdbcConnectionUrl(String jdbcConnectionUrl) {
        this.jdbcConnectionUrl = jdbcConnectionUrl;
    }

    /**
     * Returns {@link String} fully qualified class name of jdbc driver.
     *
     * @return fully qualified class name of jdbc driver
     */
    public String getDriverClassName() {
        return driverClassName;
    }

    /**
     * Sets {@link String} fully qualified class name of jdbc driver.
     * @param driverClassName fully qualified class name of jdbc driver
     */
    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    /**
     * Returns {@link String} login of db user.
     *
     * @return login of db user
     */
    public String getLogin() {
        return login;
    }

    /**
     * Sets {@link String} login of db user.
     * @param login login of db user
     */
    public void setLogin(String login) {
        this.login = login;
    }

    /**
     * Returns {@link String} password of db user.
     *
     * @return password of db user
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets {@link String} password of db user.
     * @param password password of db user
     */
    public void setPassword(String password) {
        this.password = password;
    }

}
