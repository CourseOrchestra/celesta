package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;

import java.util.*;


/**
 * Created by ioann on 01.06.2017.
 */
public final class FieldsLookup {

  final private Cursor cursor;
  final private Cursor otherCursor;

  final private List<String> fields = new ArrayList<>();
  final private List<String> otherFields = new ArrayList<>();


  public FieldsLookup(Cursor cursor, Cursor otherCursor) {
    this.cursor = cursor;
    this.otherCursor = otherCursor;
  }


  public FieldsLookup add(String field, String otherField) {
    try {
      String tableTemplate = DBAdaptor.getAdaptor().tableTemplate();

      Table table = cursor.meta();
      Column column = table.getColumn(field);
      Table otherTable = otherCursor.meta();
      Column otherColumn = otherTable.getColumn(otherField);

      String errorField = "";
      Table errorTable = null;
      if (column == null) {
        errorField = field;
        errorTable = table;
      } else if (otherColumn == null) {
        errorField = otherField;
        errorTable = otherTable;
      }

      if (!errorField.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("There is no column with name \"%s\" in table " + tableTemplate,
                errorField, errorTable.getGrain().getName(), errorTable.getName())
        );
      }

      if (!column.getCelestaType().equals(otherColumn.getCelestaType())) {
        throw new IllegalArgumentException(String.format("Column type of " + tableTemplate + ".\"%s\" is not equal " +
                "to column type of " + tableTemplate + ".\"%s\"",
            table.getGrain().getName(), table.getName(), field,
            otherTable.getGrain().getName(), otherTable.getName(), otherField));
      }

      List<String> fieldsToValidate = new ArrayList<>(fields);
      fieldsToValidate.add(field);
      validateIndices(table, fieldsToValidate, true);

      fieldsToValidate = new ArrayList<>(otherFields);
      fieldsToValidate.add(otherField);
      validateIndices(otherTable, fieldsToValidate, true);

      fields.add(field);
      otherFields.add(otherField);
      return this;
    } catch (CelestaException | ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateIndices(Table table, List<String> fieldList, boolean preValidation) {
    Set<String> fieldSet = new HashSet<>(fieldList);

    Set<Index> indexes = table.getIndices();
    Optional<Index> index = indexes.stream()
        .filter(i -> {
          if (preValidation) {
            return i.getColumns().keySet().containsAll(fieldSet);
          } else {
            return i.getColumns().keySet().equals(fieldSet);
          }
        })
        .findFirst();

    if (!index.isPresent()) {
      try {
        String tableTemplate = DBAdaptor.getAdaptor().tableTemplate();
        throw new IllegalStateException(
            String.format("There is no index for column(s) (\"%s\") in table " + tableTemplate,
                String.join(",", fieldSet), table.getGrain().getName(), table.getName())
        );
      } catch (CelestaException e) {
        throw new RuntimeException(e);
      }
    }
  }


  public void validate() {
    try {
      validateIndices(cursor.meta(), fields, false);
      validateIndices(otherCursor.meta(), otherFields, false);
    } catch (CelestaException e) {
      throw new RuntimeException(e);
    }
  }


  public Cursor getCursor() {
    return cursor;
  }

  public Cursor getOtherCursor() {
    return otherCursor;
  }

  public List<String> getFields() {
    return new ArrayList<>(fields);
  }

  public List<String> getOtherFields() {
    return new ArrayList<>(otherFields);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;

    if (!(o instanceof FieldsLookup)) return false;

    FieldsLookup other = (FieldsLookup) o;

    return Objects.equals(this.cursor.getClass().getName(), other.cursor.getClass().getName()) &&
        Objects.equals(this.otherCursor.getClass().getName(), other.otherCursor.getClass().getName()) &&
        this.fields.equals(other.fields) &&
        this.otherCursor.equals(other.fields);
  }

  @Override
  public int hashCode() {
    int result = 17;

    result += 31 * cursor.getClass().getName().hashCode();
    result += 31 * otherCursor.getClass().getName().hashCode();

    return result;
  }
}
