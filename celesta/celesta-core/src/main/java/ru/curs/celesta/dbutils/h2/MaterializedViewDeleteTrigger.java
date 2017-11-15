package ru.curs.celesta.dbutils.h2;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by ioann on 07.07.2017.
 */
public class MaterializedViewDeleteTrigger extends AbstractMaterializeViewTrigger {

  public static final String NAME_PREFIX = "mvDeleteFrom";

  @Override
  public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
    delete(connection, oldRow);
    insert(connection, oldRow);
  }

  @Override
  String getNamePrefix() {
    return NAME_PREFIX;
  }
}
