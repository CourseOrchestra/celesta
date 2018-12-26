package ru.curs.celesta.dbutils.h2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Update trigger of materialized view.
 *
 * @author ioann
 * @since 2017-07-07
 */
public final class MaterializedViewUpdateTrigger extends AbstractMaterializeViewTrigger {

  private static final String NAME_PREFIX = "mvUpdateFrom";


  @Override
  public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
    if (mvColumnsAreChanged(oldRow, newRow)) {
      //process old
      delete(connection, oldRow);
      insert(connection, oldRow);
      //process new
      delete(connection, newRow);
      insert(connection, newRow);
    }
  }

  @Override
  String getNamePrefix() {
    return NAME_PREFIX;
  }

  private boolean mvColumnsAreChanged(Object[] oldRow, Object[] newRow) {
      return getMvColumnRefs().keySet().stream()
        .anyMatch(i -> !Objects.equals(oldRow[i], newRow[i]));
  }

}
