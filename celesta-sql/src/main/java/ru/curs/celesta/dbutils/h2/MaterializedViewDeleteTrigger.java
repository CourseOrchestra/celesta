package ru.curs.celesta.dbutils.h2;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Delete trigger of materialized view.
 *
 * @author ioann
 * @since 2017-07-07
 */
public final class MaterializedViewDeleteTrigger extends AbstractMaterializeViewTrigger {

  private static final String NAME_PREFIX = "mvDeleteFrom";

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
