package ru.curs.celesta.dbutils.h2;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by ioann on 07.07.2017.
 */
public class MaterializedViewInsertTrigger extends AbstractMaterializeViewTrigger {

  public static final String NAME_PREFIX = "mvInsertFrom";

  @Override
  public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
    delete(connection, newRow);
    insert(connection, newRow);
  }

  @Override
  String getNamePrefix() {
    return NAME_PREFIX;
  }
}
