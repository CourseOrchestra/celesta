package ru.curs.celesta.score;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Объект-таблица в метаданных.
 * 
 */
public final class Table extends NamedElement {

	/**
	 * Гранула, к которой относится данная таблица.
	 */
	private final Grain grain;

	private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
		@Override
		String getErrorMsg(String name) {
			return String.format(
					"Column '%s' defined more than once in table '%s'.", name,
					getName());
		}

	};
	private final NamedElementHolder<Column> pk = new NamedElementHolder<Column>() {
		@Override
		String getErrorMsg(String name) {
			return String
					.format("Column '%s' defined more than once for primary key in table '%s'.",
							name, getName());
		}

	};

	private final Set<ForeignKey> fKeys = new LinkedHashSet<>();

	private boolean pkFinalized = false;

	private String pkConstraintName;

	public Table(Grain grain, String name) throws ParseException {
		super(name);
		if (grain == null)
			throw new IllegalArgumentException();
		this.grain = grain;
		grain.addTable(this);
	}

	/**
	 * Неизменяемый перечень столбцов таблицы.
	 */
	public Map<String, Column> getColumns() {
		return columns.getElements();
	}

	/**
	 * Неизменяемый перечень столбцов первичного ключа таблицы.
	 */
	public Map<String, Column> getPrimaryKey() {
		return pk.getElements();
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
		if (column.getParentTable() != this)
			throw new IllegalArgumentException();
		columns.addElement(column);
	}

	@Override
	public String toString() {
		return "name: " + getName() + " " + columns.toString();
	}

	/**
	 * Устанавливает первичный ключ для таблицы в виде массива колонок.
	 * Используется для динамического управления метаданными.
	 * 
	 * @param columns
	 *            перечень колонок
	 * @throws ParseException
	 *             в случае, когда передаётся пустой перечень
	 */
	public void setPK(Column[] columns) throws ParseException {
		// TODO реализовать
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
					"Column '%s' is not defined in table '%s'.", name,
					getName()));
		if (c.isNullable())
			throw new ParseException(String.format(
					"Column '%s' is nullable and therefore it cannot be "
							+ "a part of a primary key in table '%s'.", name,
					getName()));
		if (c instanceof BinaryColumn)
			throw new ParseException(
					String.format(
							"Column %s is of long binary type and therefore "
									+ "it cannot a part of a primary key in table '%s'.",
							name, getName()));

		pk.addElement(c);
	}

	void addFK(ForeignKey fk) throws ParseException {
		if (fk.getParentTable() != this)
			throw new IllegalArgumentException();
		if (fKeys.contains(fk)) {
			StringBuilder sb = new StringBuilder();
			for (Column c : fk.getColumns().values()) {
				if (sb.length() != 0)
					sb.append(", ");
				sb.append(c.getName());
			}
			throw new ParseException(
					String.format(
							"Foreign key with columns %s is already defined in table '%s'",
							sb.toString(), getName()));
		}
		fKeys.add(fk);
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

	/**
	 * Возвращает гранулу, к которой относится таблица.
	 */
	public Grain getGrain() {
		return grain;
	}

	/**
	 * Возвращает перечень внешних ключей таблицы.
	 */
	public Set<ForeignKey> getForeignKeys() {
		return Collections.unmodifiableSet(fKeys);
	}

	/**
	 * Возвращает имя ограничения PK (или null, если оно не задано).
	 */
	public String getPkConstraintName() {
		return pkConstraintName;
	}

	void setPkConstraintName(String pkConstraintName) {
		this.pkConstraintName = pkConstraintName;
	}

}
