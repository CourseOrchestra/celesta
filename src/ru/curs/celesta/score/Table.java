package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Объект-таблица в метаданных.
 * 
 */
public final class Table extends NamedElement {

	private static final String YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO = "Table '%s.%s', "
			+ "field '%s': you cannot drop a column that belongs to ";

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
	 * Возвращает столбец по его имени, либо исключение с сообщением о том, что
	 * столбец не найден.
	 * 
	 * @param name
	 *            Имя
	 * @throws ParseException
	 *             Если столбец с таким именем не найден в таблице.
	 */
	public Column getColumn(String name) throws ParseException {
		Column result = columns.get(name);
		if (result == null)
			throw new ParseException(String.format(
					"Column '%s' not found in table '%s.%s'", name, getGrain()
							.getName(), getName()));
		return result;
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
		grain.modify();
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
	 * @param columnNames
	 *            перечень колонок
	 * @throws ParseException
	 *             в случае, когда передаётся пустой перечень
	 */
	public void setPK(String... columnNames) throws ParseException {
		if (columnNames == null || columnNames.length == 0)
			throw new ParseException(String.format(
					"Primary key for table %s.%s cannot be empty.",
					grain.getName(), getName()));
		for (String n : columnNames)
			validatePKColumn(n);
		grain.modify();
		pk.clear();
		pkFinalized = false;
		for (String n : columnNames)
			addPK(n);
		finalizePK();
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
		Column c = validatePKColumn(name);
		pk.addElement(c);
	}

	private Column validatePKColumn(String name) throws ParseException {
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
		if (c instanceof StringColumn && ((StringColumn) c).isMax())
			throw new ParseException(
					String.format(
							"Column '%s' is of nvarchar(max) type and therefore "
									+ "it cannot a part of a primary key in table '%s'.",
							name, getName()));

		return c;
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
		grain.modify();
		fKeys.add(fk);
	}

	synchronized void removeFK(ForeignKey foreignKey) throws ParseException {
		grain.modify();
		fKeys.remove(foreignKey);
	}

	synchronized void removeColumn(Column column) throws ParseException {
		// Составную часть первичного ключа нельзя удалить
		if (pk.contains(column))
			throw new ParseException(String.format(
					YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO
							+ "a primary key. Change primary key first.",
					getGrain().getName(), getName(), column.getName()));
		// Составную часть индекса нельзя удалить
		for (Index ind : getGrain().getIndices().values())
			if (ind.getColumns().containsValue(column))
				throw new ParseException(
						String.format(
								YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO
										+ "an index. Drop or change relevant index first.",
								getGrain().getName(), getName(),
								column.getName()));
		// Составную часть внешнего ключа нельзя удалить
		for (ForeignKey fk : fKeys)
			if (fk.getColumns().containsValue(column))
				String.format(
						YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO
								+ "a foreign key. Drop or change relevant foreign key first.",
						getGrain().getName(), getName(), column.getName());

		grain.modify();
		columns.remove(column);
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
		return pkConstraintName == null ? "pk_" + getName() : pkConstraintName;
	}

	void setPkConstraintName(String pkConstraintName) {
		this.pkConstraintName = pkConstraintName;
	}

	/**
	 * Удаляет таблицу.
	 * 
	 * @throws ParseException
	 *             при попытке изменить системную гранулу
	 */
	public void delete() throws ParseException {
		grain.removeTable(this);
	}

	void save(BufferedWriter bw) throws IOException {
		Grain.writeCelestaDoc(this, bw);
		bw.write(String.format("CREATE TABLE %s(", getName()));
		bw.newLine();
		boolean singlePK = pk.size() == 1;
		boolean comma = false;
		for (Column c : getColumns().values()) {
			if (comma) {
				bw.write(",");
				bw.newLine();
			}
			c.save(bw);
			if (singlePK && pk.contains(c))
				bw.write(" PRIMARY KEY");
			comma = true;
		}
		bw.newLine();
		if (!singlePK) {
			bw.write("  PRIMARY KEY (");
			comma = false;
			for (Column c : getPrimaryKey().values()) {
				if (comma)
					bw.write(", ");
				bw.write(c.getName());
				comma = true;
			}
			bw.write(");");
			bw.newLine();
		}
		bw.write(");");
		bw.newLine();
		bw.newLine();
	}
}
