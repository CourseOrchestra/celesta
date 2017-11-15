package ru.curs.celesta.score;

import java.util.Map;

/**
 * Интерфейс, определяющий метасущность, как таблицу в бд
 *
 * Created by ioann on 13.06.2017.
 */
public interface TableElement {

  String YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO = "Table '%s.%s', "
      + "field '%s': you cannot drop a column that belongs to ";

  /**
   * Возвращает гранулу, к которой относится табличный элемент.
   */
  Grain getGrain();

  /**
   * Возвращает имя табличного элемента.
   */
  String getName();

  /**
   * Возвращает имя в прямых кавычках ("ANSI quotes").
   */
  String getQuotedName();

  /**
   * Перечень столбцов с именами.
   */
  Map<String, Column> getColumns();

  /**
   * Возвращает столбец по его имени, либо исключение с сообщением о том, что
   * столбец не найден.
   *
   * @param colName
   *            Имя
   * @throws ParseException
   *             Если столбец с таким именем не найден в таблице.
   */
  Column getColumn(String colName) throws ParseException;

  void addColumn(Column column) throws ParseException;

  void removeColumn(Column column) throws ParseException;

  boolean hasPrimeKey();

  /**
   * Возвращает имя ограничения PK (или null, если оно не задано).
   */
  String getPkConstraintName();

  /**
   * Неизменяемый перечень столбцов первичного ключа таблицы.
   */
  Map<String, Column> getPrimaryKey();
}
