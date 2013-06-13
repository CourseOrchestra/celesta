package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Объект-таблица в метаданных.
 * 
 */
public final class Table extends NamedElement {

	public Table(String name) {
		super(name);
	}

	private final Map<String, Column> columns = new LinkedHashMap<>();
	private final Map<String, Column> pk = new LinkedHashMap<>();
	private boolean pkFinalized = false;

	/**
	 * Неизменяемый перечень столбцов таблицы.
	 */
	public Map<String, Column> getColumns() {
		return Collections.unmodifiableMap(columns);
	}

	/**
	 * Неизменяемый перечень столбцов первичного ключа таблицы.
	 */
	public Map<String, Column> getPrimaryKey() {
		return Collections.unmodifiableMap(pk);
	}

	/**
	 * Добавляет колонку к таблице.
	 * 
	 * @param column
	 *            Новая колонка.
	 * @throws ParseException
	 *             Если колонка с таким именем уже определена.
	 */
	void addColumn(Column column) throws ParseException {
		Column oldValue = columns.put(column.getName(), column);
		if (oldValue != null) {
			columns.put(oldValue.getName(), oldValue);
			throw new ParseException(String.format(
					"Column '%s' defined more than once in table '%s'.",
					column.getName(), getName()));
		}
	}

	@Override
	public String toString() {
		return "name: " + getName() + " " + columns.toString();
	}

	/**
	 * Добавляет колонку первичного ключа.
	 * 
	 * @param string
	 *            Имя колонки первичного ключа.
	 */
	void addPK(String name) throws ParseException {
		if (pkFinalized)
			throw new ParseException(String.format(
					"More than one PRIMARY KEY definition in table '%s'.",
					getName()));
		Column c = columns.get(name);
		if (c == null)
			throw new ParseException(String.format(
					"Column %s is not defined in table '%s'.", name, getName()));
		if (c.isNullable())
			throw new ParseException(String.format(
					"Column %s is nullable and therefore it cannot be "
							+ "a part of a primary key in table '%s'.", name,
					getName()));
		if (c instanceof BinaryColumn)
			throw new ParseException(
					String.format(
							"Column %s is of long binary type and therefore "
									+ "it cannot a part of a primary key in table '%s'.",
							name, getName()));

		if (pk.put(name, c) != null)
			throw new ParseException(
					String.format(
							"Column '%s' defined more than once for primary key in table '%s'.",
							name, getName()));
	}

	/**
	 * Финализирует создание первичного ключа.
	 * 
	 * @throws ParseException
	 *             Если первичный ключ пуст.
	 */
	void finalizePK() throws ParseException {
		if (pk.isEmpty())
			throw new ParseException(String.format(
					"No primary key defined for table %s!", getName()));
		pkFinalized = true;
	}

}
