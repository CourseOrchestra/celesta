package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Табличный индекс. Celesta допускает создание только простых индексов, без
 * ограничения UNIQUE.
 */
public class Index extends GrainElement {
	private static final String INDEX_CREATION_ERROR = "Error while creating index '%s': column '%s' in table '%s' is ";
	private final Table table;
	private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
		@Override
		protected String getErrorMsg(String name) {
			return String.format("Column '%s' is defined more than once in index '%s'", name, getName());
		}
	};

	Index(Grain grain, String tableName, String name) throws ParseException {
		super(grain, name);
		if (tableName == null || name == null)
			throw new IllegalArgumentException();
		table = grain.getElement(tableName, Table.class);
		if (table == null)
			throw new ParseException(
					String.format("Error while creating index '%s': table '%s' not found.", name, tableName));
		grain.addIndex(this);
		table.addIndex(this);
	}

	public Index(Table t, String name, String[] columns) throws ParseException {
		this(t.getGrain(), t.getName(), name);
		for (String n : columns)
			addColumn(n);
		finalizeIndex();
	}

	/**
	 * Таблица индекса.
	 */
	public Table getTable() {
		return table;
	}

	/**
	 * Добавляет колонку к индексу.
	 * 
	 * @param columnName
	 *            Имя колонки (такая колонка должна существовать в таблице).
	 * @throws ParseException
	 *             В случае, если колонка не найдена, или уже встречается в
	 *             индексе, или имеет тип IMAGE.
	 */
	void addColumn(String columnName) throws ParseException {
		if (columnName == null)
			throw new IllegalArgumentException();
		Column c = table.getColumns().get(columnName);
		if (c == null)
			throw new ParseException(
					String.format(INDEX_CREATION_ERROR + "not defined.", getName(), columnName, table.getName()));
		if (c instanceof BinaryColumn)
			throw new ParseException(String.format(
					INDEX_CREATION_ERROR + "of long binary type and therefore cannot be a part of an index.", getName(),
					columnName, table.getName()));
		if (c instanceof StringColumn && ((StringColumn) c).isMax())
			throw new ParseException(
					String.format(INDEX_CREATION_ERROR + "of TEXT type and therefore cannot be a part of an index.",
							getName(), columnName, table.getName()));

		if (c.isNullable()) {
			String err = String.format(
					"WARNING for index '%s': column '%s' in table '%s' is nullable and this can affect performance.",
					getName(), columnName, table.getName());
			System.out.println(err);
		}

		columns.addElement(c);
	}

	/**
	 * Финализирует индекс.
	 * 
	 * @throws ParseException
	 *             В случае, если на этой таблице обнаружен индекс,
	 *             дублирующийся по составу полей.
	 */
	void finalizeIndex() throws ParseException {
		if (Arrays.equals(
				getColumns().entrySet().toArray(),
				table.getPrimaryKey().entrySet().toArray()
		)) {
			throw new ParseException(
					String.format("Can't add index %s to table %s.%s. " +
									"Primary key with same columns and order already exists." ,
							getName(), table.getGrain().getName(), table.getName())
			);
		}
		// Ищем дублирующиеся по составу полей индексы
		for (Index ind : getGrain().getIndices().values()) {
			if (ind == this)
				continue;
			if (ind.table != table)
				continue;
			if (ind.columns.size() != columns.size())
				continue;
			Iterator<Column> i = ind.columns.iterator();
			boolean coincide = true;
			for (Column c : columns)
				if (c != i.next()) {
					coincide = false;
					break;
				}
			if (coincide)
				throw new ParseException(
						String.format("Error while creating index '%s': it is duplicate of index '%s' for table '%s'",
								getName(), ind.getName(), table.getName()));

		}
	}

	/**
	 * Колонки индекса.
	 */
	public Map<String, Column> getColumns() {
		return columns.getElements();
	}

	/**
	 * Удаляет индекс.
	 * 
	 * @throws ParseException
	 *             при попытке изменить системную гранулу
	 */
	public void delete() throws ParseException {
		getGrain().removeIndex(this);
	}

	@Override
	void save(BufferedWriter bw) throws IOException {
		Grain.writeCelestaDoc(this, bw);
		bw.write("CREATE INDEX ");
		bw.write(getName());
		bw.write(" ON ");
		bw.write(table.getName());
		bw.write("(");
		boolean comma = false;
		for (Column c : columns) {
			if (comma)
				bw.write(", ");
			bw.write(c.getName());
			comma = true;
		}
		bw.write(");");
		bw.newLine();
	}

	@Override
	public int getColumnIndex(String name) {
		return columns.getIndex(name);
	}
}
