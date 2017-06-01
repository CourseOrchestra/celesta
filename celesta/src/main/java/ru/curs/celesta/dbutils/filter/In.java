package ru.curs.celesta.dbutils.filter;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.score.Table;


/**
 * Created by ioann on 01.06.2017.
 */
public final class In extends AbstractFilter {

  final private FieldsLookup lookup;

  public In(FieldsLookup lookup) {
    this.lookup = lookup;
  }

  public FieldsLookup getLookup() {
    return lookup;
  }

  @Override
  public boolean filterEquals(AbstractFilter o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof In)) return false;

    In other = (In) o;
    return this.lookup.equals(other.lookup);
  }

  @Override
  public String toString() {
    try {
      String template = "( %s ) from table %s IN ( %s ) from table %s )";

      String fieldsStr = String.join(",", lookup.getFields());
      String otherFieldsStr = String.join(",", lookup.getOtherFields());

      Table table = lookup.getCursor().meta();
      String tableStr = String.format("%s.%s", table.getGrain().getName(), table.getName());

      Table otherTable = lookup.getOtherCursor().meta();
      String otherTableStr = String.format("%s.%s", otherTable.getGrain().getName(), otherTable.getName());

      String result = String.format(template, fieldsStr, tableStr, otherFieldsStr, otherTableStr);
      return result;
    } catch (CelestaException e) {
      throw new RuntimeException(e);
    }
  }

}


