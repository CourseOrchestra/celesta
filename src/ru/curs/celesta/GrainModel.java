package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class GrainModel {
	private final Map<String, Table> tables = new LinkedHashMap<>();

	/**
	 * Возвращает набор таблиц, определённый в грануле.
	 */
	public Map<String, Table> getTables() {
		return Collections.unmodifiableMap(tables);
	}

	@Override
	public String toString() {
		return tables.toString();
	}

	/**
	 * Добавляет таблицу.
	 * 
	 * @param table
	 *            Новая таблица гранулы.
	 * @throws ParseException
	 *             В случае, если таблица с таким именем уже существует.
	 */
	void addTable(Table table) throws ParseException {
		if (tables.put(table.getName(), table) != null)
			throw new ParseException(String.format(
					"Table '%s' defined more than once in a grain.",
					table.getName()));

	}

}
