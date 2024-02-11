package ru.curs.celesta;

/**
 * Connection pool configuration.
 */
public final class ConnectionPoolConfiguration {

  private String jdbcConnectionUrl;
  private String login;
  private String password;

  /**
   * Returns JDBC connection URL.
   * @return
   */
  public String getJdbcConnectionUrl() {
    return jdbcConnectionUrl;
  }

  /**
   * Sets JDBC connection URL.
   * @param jdbcConnectionUrl  JDBC connection URL
   */
  public void setJdbcConnectionUrl(String jdbcConnectionUrl) {
    this.jdbcConnectionUrl = jdbcConnectionUrl;
  }

  /**
   * Returns user login.
   * @return
   */
  public String getLogin() {
    return login;
  }

  /**
   * Sets user login.
   * @param login  user login.
   */
  public void setLogin(String login) {
    this.login = login;
  }

  /**
   * Returns password.
   * @return
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets password.
   * @param password  password
   */
  public void setPassword(String password) {
    this.password = password;
  }

}
