package ru.curs.celesta;

import java.util.Map;

public final class Grain {
	private final NamedElementHolder<Table> tables = new NamedElementHolder<Table>() {
		@Override
		String getErrorMsg(String name) {
			return String.format(
					"Table '%s' defined more than once in a grain.", name);
		}

	};

	private final NamedElementHolder<Index> indices = new NamedElementHolder<Index>() {
		@Override
		String getErrorMsg(String name) {
			return String.format(
					"Index '%s' defined more than once in a grain.", name);
		}
	};

	/**
	 * Возвращает набор таблиц, определённый в грануле.
	 */
	public Map<String, Table> getTables() {
		return tables.getElements();
	}

	/**
	 * Возвращает набор индексов, определённых в грануле.
	 */
	public Map<String, Index> getIndices() {
		return indices.getElements();
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
		if (table.getGrain() != this)
			throw new IllegalArgumentException();
		tables.addElement(table);
	}

	/**
	 * Добавляет индекс.
	 * 
	 * @param index
	 *            Новый индекс гранулы.
	 * @throws ParseException
	 *             В случае, если индекс с таким именем уже существует.
	 */
	public void addIndex(Index index) throws ParseException {
		if (index.getGrain() != this)
			throw new IllegalArgumentException();
		indices.addElement(index);
	}

}
