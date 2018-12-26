package ru.curs.celesta.dbutils.h2;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * Insert trigger of materialized view.
 *
 * @author ioann
 * @since 2017-07-07
 */
public final class MaterializedViewInsertTrigger extends AbstractMaterializeViewTrigger {

  private static final String NAME_PREFIX = "mvInsertFrom";

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
