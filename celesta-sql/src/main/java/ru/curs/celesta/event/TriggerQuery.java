package ru.curs.celesta.event;

/**
 * Trigger parameter object.
 *
 * @author ioann
 * @since 2017-07-03
 */
public final class TriggerQuery {

  private String schema;
  private String name;
  private String tableName;
  private TriggerType type;

  /**
   * Sets DB schema name parameter.
   *
   * @param schema  schema name
   * @return  {@code this}
   */
  public TriggerQuery withSchema(String schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Sets trigger name parameter.
   *
   * @param name  trigger name
   * @return  {@code this}
   */
  public TriggerQuery withName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Sets table name parameter.
   *
   * @param tableName  table name
   * @return {@code this}
   */
  public TriggerQuery withTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  /**
   * Sets trigger type parameter.
   *
   * @param type  trigger type
   * @return {@code this}
   */
  public TriggerQuery withType(TriggerType type) {
    this.type = type;
    return this;
  }

  /**
   * Returns schema name parameter.
   *
   * @return
   */
  public String getSchema() {
    return schema;
  }

  /**
   * Returns trigger name parameter.
   *
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * Returns table name parameter.
   *
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns trigger type parameter.
   *
   * @return
   */
  public TriggerType getType() {
    return type;
  }

}
