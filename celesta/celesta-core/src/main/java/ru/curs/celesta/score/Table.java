package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Объект-таблица в метаданных.
 * 
 */
public final class Table extends DataGrainElement implements TableElement, VersionedElement {

	private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
		@Override
		protected String getErrorMsg(String name) {
			return String.format("Column '%s' defined more than once in table '%s'.", name, getName());
		}

	};
	private final NamedElementHolder<Column> pk = new NamedElementHolder<Column>() {
		@Override
		protected String getErrorMsg(String name) {
			return String.format("Column '%s' defined more than once for primary key in table '%s'.", name, getName());
		}

	};

	private final Set<ForeignKey> fKeys = new LinkedHashSet<>();

	private final Set<Index> indices = new HashSet<>();

	private final IntegerColumn recVersion = new IntegerColumn(this);

	private boolean pkFinalized = false;

	private boolean isReadOnly = false;

	private boolean isVersioned = true;

	private boolean autoUpdate = true;

	private String pkConstraintName;

	public Table(Grain grain, String name) throws ParseException {
		super(grain, name);
		grain.addElement(this);
	}

	/**
	 * Неизменяемый перечень столбцов таблицы.
	 */
	@Override
	public Map<String, Column> getColumns() {
		return columns.getElements();
	}


	@Override
	public Column getColumn(String colName) throws ParseException {
		Column result = columns.get(colName);
		if (result == null)
			throw new ParseException(
					String.format("Column '%s' not found in table '%s.%s'", colName, getGrain().getName(), getName()));
		return result;
	}


	@Override
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
	@Override
	public void addColumn(Column column) throws ParseException {
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
			throw new ParseException(
					String.format("Primary key for table %s.%s cannot be empty.", getGrain().getName(), getName()));
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
	 * @param name
	 *            Имя колонки первичного ключа.
	 */
	void addPK(String name) throws ParseException {
		if (pkFinalized)
			throw new ParseException(String.format("More than one PRIMARY KEY definition in table '%s'.", getName()));
		Column c = validatePKColumn(name);
		pk.addElement(c);
	}

	private Column validatePKColumn(String name) throws ParseException {
		if (VersionedElement.REC_VERSION.equals(name))
			throw new ParseException(String.format("Column '%s' is not allowed for primary key.", name));
		Column c = columns.get(name);
		if (c == null)
			throw new ParseException(String.format("Column '%s' is not defined in table '%s'.", name, getName()));
		if (c.isNullable())
			throw new ParseException(String.format(
					"Column '%s' is nullable and therefore it cannot be " + "a part of a primary key in table '%s'.",
					name, getName()));
		if (c instanceof BinaryColumn)
			throw new ParseException(String.format("Column %s is of long binary type and therefore "
					+ "it cannot a part of a primary key in table '%s'.", name, getName()));
		if (c instanceof StringColumn && ((StringColumn) c).isMax())
			throw new ParseException(String.format(
					"Column '%s' is of TEXT type and therefore " + "it cannot a part of a primary key in table '%s'.",
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
			throw new ParseException(String.format("Foreign key with columns %s is already defined in table '%s'",
					sb.toString(), getName()));
		}
		getGrain().modify();
		fKeys.add(fk);
	}

	synchronized void removeFK(ForeignKey foreignKey) throws ParseException {
		getGrain().modify();
		fKeys.remove(foreignKey);
	}

	@Override
	public synchronized void removeColumn(Column column) throws ParseException {
		// Составную часть первичного ключа нельзя удалить
		if (pk.contains(column))
			throw new ParseException(
					String.format(YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "a primary key. Change primary key first.",
							getGrain().getName(), getName(), column.getName()));
		// Составную часть индекса нельзя удалить
		for (Index ind : getGrain().getIndices().values())
			if (ind.getColumns().containsValue(column))
				throw new ParseException(String.format(
						YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "an index. Drop or change relevant index first.",
						getGrain().getName(), getName(), column.getName()));
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
			throw new ParseException(String.format("No primary key defined for table %s!", getName()));
		pkFinalized = true;
	}

	private void throwPE(String option) throws ParseException {
		throw new ParseException(
				String.format(
						"Invalid option for table '%s': '%s'. 'READ ONLY', "
								+ "'VERSION CHECK', 'NO VERSION CHECK', and/or 'NO AUTOUPDATE' expected.",
						getName(), option));
	}

	/**
	 * Финализирует таблицу с перечнем WITH-опций.
	 * 
	 * @param options
	 *            Перечень опций.
	 * @throws ParseException
	 *             Ошибка определения таблицы.
	 */
	// CHECKSTYLE:OFF for cyclomatic complexity: this is finite state machine
	public void finalizePK(List<String> options) throws ParseException {
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
				// 'no' read for the first time
				if ("version".equalsIgnoreCase(option)) {
					state = 3;
					isReadOnly = false;
					isVersioned = false;
				} else if ("autoupdate".equalsIgnoreCase(option)) {
					state = 7;
					autoUpdate = false;
				} else {
					throwPE(option);
				}
				break;
			case 5:
				if ("no".equalsIgnoreCase(option)) {
					state = 6;
				} else {
					throwPE(option);
				}
				break;
			case 6:
				if ("autoupdate".equalsIgnoreCase(option)) {
					state = 7;
					autoUpdate = false;
				} else {
					throwPE(option);
				}
				break;
			case 7:
				throwPE(option);
				break;
			default:
				break;
			}
		if (state == 0 || state == 5 || state == 7) {
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
	 * Возвращает перечень индексов таблицы.
	 */
	public Set<Index> getIndices() {
		return Collections.unmodifiableSet(indices);
	}


	@Override
	public boolean hasPrimeKey() {
		return true;
	}

	@Override
	public String getPkConstraintName() {
		return pkConstraintName == null ? limitName("pk_" + getName()) : pkConstraintName;
	}

	/**
	 * Устанавливает имя ограничения PK.
	 * 
	 * @param pkConstraintName
	 *            имя
	 * @throws ParseException
	 *             неверное имя
	 */
	public void setPkConstraintName(String pkConstraintName) throws ParseException {
		if (pkConstraintName != null)
			validateIdentifier(pkConstraintName);
		this.pkConstraintName = pkConstraintName;
	}

	@Override
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
		boolean withEmitted = false;
		if (isReadOnly) {
			bw.write(" WITH READ ONLY");
			withEmitted = true;
		} else if (!isVersioned) {
			bw.write(" WITH NO VERSION CHECK");
			withEmitted = true;
		}
		if (!autoUpdate) {
			if (!withEmitted)
				bw.write(" WITH");
			bw.write(" NO AUTOUPDATE");
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
			throw new ParseException(String.format(
					"Method setReadOnly(true) failed: table %s should be either versioned or read only.", getName()));

		this.isReadOnly = isReadOnly;
	}

	@Override
	public boolean isVersioned() {
		return isVersioned;
	}

	@Override
	public IntegerColumn getRecVersionField() {
		return recVersion;
	}

	/**
	 * Значение false yказывает на то, что таблица создана с опцией WITH NO
	 * STRUCTURE UPDATE и не будет участвовать в автообновлении базы данных. По
	 * умолчанию - true.
	 */
	public boolean isAutoUpdate() {
		return autoUpdate;
	}

	/**
	 * Устанавливает или сбрасывает опцию WITH NO STRUCTURE UPDATE.
	 * 
	 * @param autoUpdate
	 *            true, если таблица автоматически обновляется, false - в
	 *            обратном случае.
	 */
	public void setAutoUpdate(boolean autoUpdate) {
		this.autoUpdate = autoUpdate;
	}

	@Override
	public int getColumnIndex(String name) {
		return columns.getIndex(name);
	}

	void addIndex(Index index) {
		indices.add(index);
	}

	void removeIndex(Index index) {
		indices.remove(index);
	}
}
