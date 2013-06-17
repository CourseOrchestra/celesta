package ru.curs.celesta;

import java.util.Map;

/**
 * Гранула.
 * 
 */
public final class Grain extends NamedElement {

	private final Score score;

	private String version;

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

	public Grain(Score score, String name) throws ParseException {
		super(name);
		if (score == null)
			throw new IllegalArgumentException();
		this.score = score;
		score.addGrain(this);
	}

	/**
	 * Возвращает набор таблиц, определённый в грануле.
	 */
	public Map<String, Table> getTables() {
		return tables.getElements();
	}

	/**
	 * Возвращает таблицу по её имени, либо исключение с сообщением о том, что
	 * таблица не найдена.
	 * 
	 * @param name
	 *            Имя
	 * @throws ParseException
	 *             Если таблица с таким именем не найдена в грануле.
	 */
	public Table getTable(String name) throws ParseException {
		Table result = tables.get(name);
		if (result == null)
			throw new ParseException(String.format(
					"Table '%s' not found in grain '%s'", name, getName()));
		return result;
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

	/**
	 * Возвращает модель, к которой принадлежит гранула.
	 */
	public Score getScore() {
		return score;
	}

	/**
	 * Возвращает номер версии гранулы.
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * Устанавливает версию гранулы.
	 * 
	 * @param version
	 *            Quoted-string. В процессе установки обрамляющие и двойные
	 *            кавычки удаляются.
	 * @throws ParseException
	 *             в случае, если имеется неверный формат quoted string.
	 */
	void setVersion(String version) throws ParseException {
		this.version = StringColumn.unquoteString(version);
	}

}
