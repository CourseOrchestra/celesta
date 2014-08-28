package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Объект-таблица в метаданных.
 * 
 */
public final class Table extends GrainElement {

	/**
	 * Имя системного поля, содержащего версию записи.
	 */
	public static final String RECVERSION = "recversion";

	private static final String YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO = "Table '%s.%s', "
			+ "field '%s': you cannot drop a column that belongs to ";

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

	private final IntegerColumn recVersion = new IntegerColumn(this);

	private boolean pkFinalized = false;

	private boolean isReadOnly = false;

	private boolean isVersioned = true;

	private String pkConstraintName;

	public Table(Grain grain, String name) throws ParseException {
		super(grain, name);
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
		getGrain().modify();
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
		if (columnNames == null || (columnNames.length == 0 && !isReadOnly))
			throw new ParseException(String.format(
					"Primary key for table %s.%s cannot be empty.", getGrain()
							.getName(), getName()));
		for (String n : columnNames)
			validatePKColumn(n);
		getGrain().modify();
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
		if (RECVERSION.equals(name))
			throw new ParseException(String.format(
					"Column '%s' is not allowed for primary key.", name));
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
							"Column '%s' is of TEXT type and therefore "
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
		getGrain().modify();
		fKeys.add(fk);
	}

	synchronized void removeFK(ForeignKey foreignKey) throws ParseException {
		getGrain().modify();
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

		getGrain().modify();
		columns.remove(column);
	}

	/**
	 * Финализирует создание первичного ключа.
	 * 
	 * @throws ParseException
	 *             Если первичный ключ пуст.
	 */
	void finalizePK() throws ParseException {
		if (pk.isEmpty() && !isReadOnly)
			throw new ParseException(String.format(
					"No primary key defined for table %s!", getName()));
		pkFinalized = true;
	}

	private void throwPE(String option) throws ParseException {
		throw new ParseException(
				String.format(
						"Invalid option for table '%s': %s. One of 'WITH READ ONLY', "
								+ "'WITH VERSION CHECK', 'WITH NO VERSION CHECK expected'.",
						getName(), option));
	}

	// CHECKSTYLE:OFF for cyclomatic complexity: this is finite state machine
	void finalizePK(List<String> options) throws ParseException {
		// CHECKSTYLE:ON
		int state = 0;
		for (String option : options)
			switch (state) {
			// beginning
			case 0:
				if ("with".equalsIgnoreCase(option)) {
					state = 1;
				} else {
					throwPE(option);
				}
				break;
			// 'with' read
			case 1:
				if ("read".equalsIgnoreCase(option)) {
					isReadOnly = true;
					isVersioned = false;
					state = 2;
				} else if ("version".equalsIgnoreCase(option)) {
					isReadOnly = false;
					isVersioned = true;
					state = 3;
				} else if ("no".equalsIgnoreCase(option)) {
					isReadOnly = false;
					isVersioned = false;
					state = 4;
				} else {
					throwPE(option);
				}
				break;
			case 2:
				if ("only".equalsIgnoreCase(option)) {
					state = 5;
				} else {
					throwPE(option);
				}
				break;
			case 3:
				if ("check".equalsIgnoreCase(option)) {
					state = 5;
				} else {
					throwPE(option);
				}
				break;
			case 4:
				if ("version".equalsIgnoreCase(option)) {
					state = 3;
				} else {
					throwPE(option);
				}
				break;
			case 5:
				throwPE(option);
			default:
				break;
			}
		if (state == 0 || state == 5) {
			finalizePK();
		} else {
			throwPE("");
		}
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
		return pkConstraintName == null ? limitName("pk_" + getName())
				: pkConstraintName;
	}

	/**
	 * Устанавливает имя ограничения PK.
	 * 
	 * @param pkConstraintName
	 *            имя
	 * @throws ParseException
	 *             неверное имя
	 */
	public void setPkConstraintName(String pkConstraintName)
			throws ParseException {
		if (pkConstraintName != null)
			validateIdentifier(pkConstraintName);
		this.pkConstraintName = pkConstraintName;
	}

	/**
	 * Удаляет таблицу.
	 * 
	 * @throws ParseException
	 *             при попытке изменить системную гранулу
	 */
	public void delete() throws ParseException {
		getGrain().removeTable(this);
	}

	void save(BufferedWriter bw) throws IOException {
		Grain.writeCelestaDoc(this, bw);
		bw.write(String.format("CREATE TABLE %s(", getName()));
		bw.newLine();
		boolean comma = false;
		for (Column c : getColumns().values()) {
			if (comma) {
				bw.write(",");
				bw.newLine();
			}
			c.save(bw);
			comma = true;
		}

		// Здесь мы пишем PK
		if (!getPrimaryKey().isEmpty()) {
			if (comma)
				bw.write(",");
			bw.newLine();
			bw.write("  CONSTRAINT ");
			bw.write(getPkConstraintName());
			bw.write(" PRIMARY KEY (");
			comma = false;
			for (Column c : getPrimaryKey().values()) {
				if (comma)
					bw.write(", ");
				bw.write(c.getName());
				comma = true;
			}
			bw.write(")");
			bw.newLine();
		}

		bw.write(")");
		if (isReadOnly)
			bw.write(" WITH READ ONLY");
		else if (!isVersioned) {
			bw.write(" WITH NO VERSION CHECK");
		}
		bw.write(";");
		bw.newLine();
		bw.newLine();
	}

	/**
	 * Является ли таблица таблицей только на чтение (WITH READ ONLY).
	 */
	public boolean isReadOnly() {
		return isReadOnly;
	}

	/**
	 * Устанавливает опцию таблицы "только для чтения".
	 * 
	 * @param isReadOnly
	 *            только для чтения.
	 * @throws ParseException
	 *             Если данная опция включается вместе с versioned.
	 */
	void setReadOnly(boolean isReadOnly) throws ParseException {
		if (isReadOnly && isVersioned)
			throw new ParseException(
					String.format(
							"Method setReadOnly(true) failed: table %s should be either versioned or read only.",
							getName()));

		this.isReadOnly = isReadOnly;
	}

	/**
	 * Является ли таблица версионированной (WITH VERSION CHECK).
	 */
	public boolean isVersioned() {
		return isVersioned;
	}

	/**
	 * Устанавливает признак версионности таблицы.
	 * 
	 * @param isVersioned
	 *            Признак версионности.
	 * @throws ParseException
	 *             Если таблица read only.
	 */
	public void setVersioned(boolean isVersioned) throws ParseException {
		if (isReadOnly && isVersioned)
			throw new ParseException(
					String.format(
							"Method setVersioned(true) failed: table %s should be either versioned or read only.",
							getName()));
		this.isVersioned = isVersioned;
	}

	/**
	 * Возвращает описание поля recversion.
	 */
	public IntegerColumn getRecVersionField() {
		return recVersion;
	}
}
