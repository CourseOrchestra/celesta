package ru.curs.celesta.event;

/**
 * Created by ioann on 03.07.2017.
 */
public class TriggerQuery {

  private String schema;
  private String name;
  private String tableName;
  private TriggerType type;

  public TriggerQuery withSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public TriggerQuery withName(String name) {
    this.name = name;
    return this;
  }

  public TriggerQuery withTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public TriggerQuery withType(TriggerType type) {
    this.type = type;
    return this;
  }

  public String getSchema() {
    return schema;
  }

  public String getName() {
    return name;
  }

  public String getTableName() {
    return tableName;
  }

  public TriggerType getType() {
    return type;
  }
}
