package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Объект-таблица в метаданных.
 * 
 */
public class Table extends NamedElement {

	public Table(String name) {
		super(name);
	}

	private final Set<Column> columns = new LinkedHashSet<>();

	/**
	 * Неизменяемый перечень столбцов таблицы.
	 */
	public Set<Column> getColumns() {
		return Collections.unmodifiableSet(columns);
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
		if (!columns.add(column))
			throw new ParseException(String.format(
					"Column '%s' defined more than once in table '%s'.",
					column.getName(), getName()));
	}

	@Override
	public String toString() {
		return "name: " + getName() + " " + columns.toString();
	}

}
