package ru.curs.celesta;

public class ConnectionPoolConfiguration {

  private String jdbcConnectionUrl;
  private String driverClassName;
  private String login;
  private String password;


  public String getJdbcConnectionUrl() {
    return jdbcConnectionUrl;
  }

  public void setJdbcConnectionUrl(String jdbcConnectionUrl) {
    this.jdbcConnectionUrl = jdbcConnectionUrl;
  }

  public String getDriverClassName() {
    return driverClassName;
  }

  public void setDriverClassName(String driverClassName) {
    this.driverClassName = driverClassName;
  }

  public String getLogin() {
    return login;
  }

  public void setLogin(String login) {
    this.login = login;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

}
