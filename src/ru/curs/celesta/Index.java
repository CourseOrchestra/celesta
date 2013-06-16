package ru.curs.celesta;

import java.util.Iterator;
import java.util.Map;

/**
 * Табличный индекс. Celesta допускает создание только простых индексов, без
 * ограничения UNIQUE.
 */
public class Index extends NamedElement {
	private final GrainModel model;
	private final Table table;
	private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
		@Override
		String getErrorMsg(String name) {
			return String.format(
					"Column '%s' is defined more than once in index '%s'",
					name, getName());
		}
	};

	public Index(GrainModel model, String tableName, String name)
			throws ParseException {
		super(name);
		if (model == null || tableName == null || name == null)
			throw new IllegalArgumentException();
		this.model = model;
		table = model.getTables().get(tableName);
		if (table == null)
			throw new ParseException(String.format(
					"Error while creating index '%s': table '%s' not found.",
					name, tableName));
		model.addIndex(this);
	}

	public GrainModel getGrainModel() {
		return model;
	}

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
	public void addColumn(String columnName) throws ParseException {
		if (columnName == null)
			throw new IllegalArgumentException();
		Column c = table.getColumns().get(columnName);
		if (c == null)
			throw new ParseException(
					String.format(
							"Error while creating index '%s': column '%s' is not defined in table '%s'",
							getName(), columnName, table.getName()));
		if (c instanceof BinaryColumn)
			throw new ParseException(
					String.format(
							"Error while creating index '%s': column '%s' in table '%s' is "
									+ "of long binary type and therefore cannot be a part of an index.",
							getName(), columnName, table.getName()));

		columns.addElement(c);
	}

	public void finalizeIndex() throws ParseException {
		// Ищем дублирующиеся по составу полей индексы
		for (Index ind : model.getIndices().values()) {
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
						String.format(
								"Error while creating index '%s': it is duplicate of index '%s' for table '%s'",
								getName(), ind.getName(), table.getName()));

		}
	}

	public Map<String, Column> getColumns() {
		return columns.getElements();
	}
}
