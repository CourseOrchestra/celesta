package ru.curs.celesta.score;

import java.util.Map;

/**
 * Interface that defines meta entity as a table in the DB.
 *
 * @author ioann
 * @since 2017-06-13
 */
public interface TableElement {

  /**
   * Constant {@code YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO}.
   */
  String YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO = "Table '%s.%s', "
      + "field '%s': you cannot drop a column that belongs to ";

  /**
   * Returns grain that the table element belongs to.
   *
   * @return
   */
  Grain getGrain();

  /**
   * Returns table element name.
   *
   * @return
   */
  String getName();

  /**
   * Returns name in ANSI quotes ("name").
   *
   * @return
   */
  String getQuotedName();

  /**
   * Returns a map of columns with names.
   *
   * @return
   */
  Map<String, Column<?>> getColumns();

  /**
   * Returns a column by its name or throws an exception with a message that
   * the column is not found.
   *
   * @param colName  column name
   * @return
   * @throws ParseException  if a column with the specified name is not found in the table
   */
  Column<?> getColumn(String colName) throws ParseException;

  /**
   * Adds a column to the table.
   *
   * @param column  new column
   * @throws ParseException  if the column couldn't be added
   */
  void addColumn(Column<?> column) throws ParseException;

  /**
   * Removes a column from the table.
   *
   * @param column  existing column
   * @throws ParseException  if the column couldn't be removed
   */
  void removeColumn(Column<?> column) throws ParseException;

  /**
   * Whether the table has primary key.
   *
   * @return
   */
  boolean hasPrimeKey();

  /**
   * Returns PK constraint name.
   *
   * @return
   */
  String getPkConstraintName();

  /**
   * Returns unmodified map of primary key columns.
   *
   * @return  map of <b>column name</b> -> <b>column</b>
   */
  Map<String, Column<?>> getPrimaryKey();

}
