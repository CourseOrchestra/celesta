package ru.curs.celesta;

import java.util.Map;

public final class GrainModel {
	private final NamedElementHolder<Table> tables = new NamedElementHolder<Table>() {
		@Override
		String getErrorMsg(String name) {
			return String.format(
					"Table '%s' defined more than once in a grain.", name);
		}

	};

	/**
	 * Возвращает набор таблиц, определённый в грануле.
	 */
	public Map<String, Table> getTables() {
		return tables.getElements();
	}

	@Override
	public String toString() {
		return tables.getElements().toString();
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
		tables.addElement(table);
	}
}
