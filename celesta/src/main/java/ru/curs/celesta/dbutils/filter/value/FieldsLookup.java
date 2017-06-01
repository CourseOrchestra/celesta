package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.dbutils.Cursor;

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
    //TODO!!! Сделать валидацию полей
    fields.add(field);
    otherFields.add(otherField);
    return this;
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
