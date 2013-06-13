package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Объект-таблица в метаданных.
 * 
 */
public class Table extends NamedElement {

	public Table(String name) {
		super(name);
	}

	private final Map<String, Column> columns = new LinkedHashMap<>();

	/**
	 * Неизменяемый перечень столбцов таблицы.
	 */
	public Map<String, Column> getColumns() {
		return Collections.unmodifiableMap(columns);
	}

	/**
	 * Добавляет колонку к таблице.
	 * 
	 * @param column
	 *            Новая колонка.
	 * @throws ParseException
	 *             Если колонка с таким именем уже определена.
	 */
	public void addColumn(Column column) throws ParseException {
		if (columns.put(column.getName(), column) != null)
			throw new ParseException(String.format(
					"Column '%s' defined more than once in table '%s'.",
					column.getName(), getName()));
	}

	@Override
	public String toString() {
		return "name: " + getName() + " " + columns.toString();
	}

}
