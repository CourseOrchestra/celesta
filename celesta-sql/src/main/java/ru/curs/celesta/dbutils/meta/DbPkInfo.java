package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.TableElement;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Information on primary key taken from the database.
 *
 * @author ioann
 * @since 2017-05-10
 */
public final class DbPkInfo {
  private final DBAdaptor dbAdaptor;  

  private String name;
  private final List<String> columnNames = new LinkedList<>();

  public DbPkInfo(DBAdaptor dbAdaptor) {
    this.dbAdaptor = dbAdaptor;
  }

  /**
   * Adds a column to the primary key.
   *
   * @param columnName  column name
   */
  public void addColumnName(String columnName) {
    columnNames.add(columnName);
  }

  /**
   * Sets primary key name.
   *
   * @param name  primary key name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns primary key name.
   *
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * Returns columns of the primary key.
   *
   * @return
   */
  public List<String> getColumnNames() {
    return columnNames;
  }

  /**
   * Whether primary key contains any columns.
   *
   * @return
   */
  public boolean isEmpty() {
    return columnNames.isEmpty();
  }

  public boolean reflects(TableElement t) {
    if (!dbAdaptor.pkConstraintString(t).equals(name)) {
        return false;
    }

    if (columnNames.size() != t.getPrimaryKey().size()) {
        return false;
    }
    Iterator<String> i1 = t.getPrimaryKey().keySet().iterator();
    Iterator<String> i2 = columnNames.iterator();
    while (i1.hasNext()) {
      if (!i1.next().equals(i2.next())) {
          return false;
      }
    }

    return true;
  }

}
