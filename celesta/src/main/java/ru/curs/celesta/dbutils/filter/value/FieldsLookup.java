package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ioann on 01.06.2017.
 */
public final class FieldsLookup {

  final private Table table;
  final private Table otherTable;

  final private List<String> fields = new ArrayList<>();
  final private List<String> otherFields = new ArrayList<>();

  public FieldsLookup(Cursor cursor, Cursor otherCursor) throws CelestaException {
    this.table = cursor.meta();
    this.otherTable = otherCursor.meta();
  }

  public FieldsLookup(Table table, Table otherTable) throws CelestaException {
    this.table = table;
    this.otherTable = otherTable;
  }


  public FieldsLookup add(String field, String otherField) throws CelestaException, ParseException {
    final String tableTemplate;

    tableTemplate = DBAdaptor.getAdaptor().tableTemplate();

    final Column column;
    final Column otherColumn;

    column = table.getColumn(field);
    otherColumn = otherTable.getColumn(otherField);

    if (!column.getCelestaType().equals(otherColumn.getCelestaType())) {
      throw new CelestaException(
          "Column type of " + tableTemplate + ".\"%s\" is not equal " + "to column type of " + tableTemplate
              + ".\"%s\"",
          table.getGrain().getName(), table.getName(), field, otherTable.getGrain().getName(),
          otherTable.getName(), otherField);
    }

    List<String> fieldsToValidate = new ArrayList<>(fields);
    fieldsToValidate.add(field);
    Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fieldsToValidate, table);

    List<String> otherFieldsToValidate = new ArrayList<>(otherFields);
    otherFieldsToValidate.add(otherField);
    Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFieldsToValidate, otherTable);

    columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

    if (columnOrdersInIndicesSet.isEmpty()) {
      throw new CelestaException("There is no indices with the same order of column(s) (\"%s\") from table table " +
          tableTemplate + "and (\"%s\") from table table" + tableTemplate,
          String.join(",", new HashSet<>(fieldsToValidate)), table.getGrain().getName(), table.getName(),
          String.join(",", new HashSet<>(otherFieldsToValidate)), otherTable.getGrain().getName(), otherTable.getName());
    }


    fields.add(field);
    otherFields.add(otherField);
    return this;
}


  public void validate() throws CelestaException {
    List<String> fieldsToValidate = new ArrayList<>(fields);
    Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fieldsToValidate, table);

    List<String> otherFieldsToValidate = new ArrayList<>(otherFields);
    Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFieldsToValidate, otherTable);

    columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

    columnOrdersInIndicesSet.stream().forEach(Collections::sort);
    Optional match = columnOrdersInIndicesSet.stream()
        .filter(l -> l.equals(IntStream.range(0, l.size()).boxed().collect(Collectors.toList())))
        .findFirst();

    if (!match.isPresent()) {
      throw new CelestaException("There are no matched indicies found for specified fields. " +
          "Positions of columns in indices must be sortable in increments of 1 starts with 0.");
    }

  }

  private Set<List<Integer>> getColumnOrdersInIndicesSet(List<String> fieldsToValidate, Table table) throws CelestaException {
    String tableTemplate = DBAdaptor.getAdaptor().tableTemplate();

    Set<Index> indexes = table.getIndices();
    List<Index> indexesToValidate = indexes.stream()
        .filter(i -> i.getColumns().keySet().containsAll(fieldsToValidate))
        .collect(Collectors.toList());

    if (indexesToValidate.isEmpty()) {
      throw new CelestaException("There is no index which contains column(s) (\"%s\") in table " + tableTemplate,
          String.join(",", new HashSet<>(fieldsToValidate)), table.getGrain().getName(), table.getName());
    }

    return indexesToValidate.stream()
        .map(i -> fieldsToValidate.stream()
            .map(f -> i.getColumnIndex(f))
            .collect(Collectors.toList())
        ).collect(Collectors.toSet());
  }

  public Table getTable() {
    return table;
  }

  public Table getOtherTable() {
    return otherTable;
  }

  public List<String> getFields() {
    return new ArrayList<>(fields);
  }

  public List<String> getOtherFields() {
    return new ArrayList<>(otherFields);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;

    if (!(o instanceof FieldsLookup))
      return false;

    FieldsLookup other = (FieldsLookup) o;

    return Objects.equals(this.table.getName(), other.table.getName())
        && Objects.equals(this.otherTable.getName(), other.otherTable.getName())
        && this.fields.equals(other.fields) && this.otherFields.equals(other.otherFields);
  }

  @Override
  public int hashCode() {
    int result = 17;

    result += 31 * table.getName().hashCode();
    result += 31 * otherTable.getName().hashCode();

    return result;
  }
}
