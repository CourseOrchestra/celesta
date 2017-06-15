package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.TableElement;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ioann on 10.05.2017.
 */
public final class DBPKInfo {
  private String name;
  private final List<String> columnNames = new LinkedList<>();

  public void addColumnName(String columnName) {
    columnNames.add(columnName);
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public boolean isEmpty() {
    return columnNames.isEmpty();
  }

  public boolean reflects(TableElement t) {
    boolean result = t.getPkConstraintName().equals(name) && (columnNames.size() == t.getPrimaryKey().size());
    Iterator<String> i1 = t.getPrimaryKey().keySet().iterator();
    Iterator<String> i2 = columnNames.iterator();
    while (result && i1.hasNext()) {
      result = i1.next().equals(i2.next());
    }
    return result;
  }
}
