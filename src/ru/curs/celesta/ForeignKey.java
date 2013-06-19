package ru.curs.celesta;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Класс внешнего ключа.
 * 
 */
public class ForeignKey {

	private final Table parentTable;
	private Table referencedTable;
	private FKBehaviour deleteBehaviour = FKBehaviour.NO_ACTION;
	private FKBehaviour updateBehaviour = FKBehaviour.NO_ACTION;
	private String constraintName;

	private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
		@Override
		String getErrorMsg(String name) {
			return String
					.format("Column '%s' defined more than once in foreign key for table '%s'.",
							name, parentTable.getName());
		}
	};

	private final List<Column> referencedColumns = new LinkedList<>();

	ForeignKey(Table parentTable) {
		if (parentTable == null)
			throw new IllegalArgumentException();
		this.parentTable = parentTable;
	}

	void setDeleteBehaviour(FKBehaviour deleteBehaviour) throws ParseException {
		if (deleteBehaviour == null)
			throw new IllegalArgumentException();
		if (deleteBehaviour == FKBehaviour.SET_NULL)
			checkNullable();
		this.deleteBehaviour = deleteBehaviour;
	}

	void setUpdateBehaviour(FKBehaviour updateBehaviour) throws ParseException {
		if (updateBehaviour == null)
			throw new IllegalArgumentException();
		if (updateBehaviour == FKBehaviour.SET_NULL)
			checkNullable();
		this.updateBehaviour = updateBehaviour;
	}

	private void checkNullable() throws ParseException {
		for (Column c : columns)
			if (!c.isNullable())
				throw new ParseException(String.format("Error while "
						+ "creating FK for table '%s': column '%s' is not "
						+ "nullable and therefore 'SET NULL' behaviour cannot "
						+ "be applied.", parentTable.getName(), c.getName()));
	}

	/**
	 * Неизменяемый перечень столбцов внешнего ключа.
	 */
	public Map<String, Column> getColumns() {
		return columns.getElements();
	}

	/**
	 * Таблица, частью которой является внешний ключ.
	 */
	public Table getParentTable() {
		return parentTable;
	}

	/**
	 * Таблица, на которую ссылается внешний ключ.
	 */
	public Table getReferencedTable() {
		return referencedTable;
	}

	/**
	 * Поведение при удалении.
	 */
	public FKBehaviour getDeleteBehaviour() {
		return deleteBehaviour;
	}

	/**
	 * Поведение при обновлении.
	 */
	public FKBehaviour getUpdateBehaviour() {
		return updateBehaviour;
	}

	/**
	 * Добавляет колонку. Колонка должна принадлежать родительской таблице.
	 * 
	 * @param columnName
	 *            имя колонки.
	 * @throws ParseException
	 *             в случае, если колонка не найдена.
	 */
	void addColumn(String columnName) throws ParseException {
		Column c = parentTable.getColumns().get(columnName);
		if (c == null)
			throw new ParseException(
					String.format(
							"Error while creating FK: no column '%s' defined in table '%s'.",
							columnName, parentTable.getName()));
		columns.addElement(c);
	}

	/**
	 * Добавляет таблицу, на которую имеется ссылка и финализирует создание
	 * первичного ключа, добавляя его к родительской таблице.
	 * 
	 * @param grain
	 *            Имя гранулы
	 * @param table
	 *            Имя таблицы
	 * @throws ParseException
	 *             В случае, если ключ с таким набором полей (хотя не
	 *             обязательно ссылающийся на ту же таблицу) уже есть в таблице.
	 */
	void setReferencedTable(String grain, String table) throws ParseException {
		// Извлечение гранулы по имени.
		Grain gm;
		if ("".equals(grain)) {
			gm = parentTable.getGrain();
		} else {
			gm = parentTable.getGrain().getScore().getGrain(grain);
		}

		// Извлечение таблицы по имени.
		Table t = gm.getTables().get(table);
		if (t == null)
			throw new ParseException(
					String.format(
							"Error while creating FK for table '%s': no table '%s' defined in grain '%s'.",
							parentTable.getName(), table, grain));
		referencedTable = t;

		// Проверка того факта, что поля ключа совпадают по типу
		// с полями первичного ключа таблицы, на которую ссылка

		Map<String, Column> refpk = referencedTable.getPrimaryKey();
		if (columns.size() != refpk.size())
			throw new ParseException(
					String.format(
							"Error creating foreign key for table %s: it has different size with PK of table '%s'",
							parentTable.getName(), referencedTable.getName()));
		Iterator<Column> i = referencedTable.getPrimaryKey().values()
				.iterator();
		for (Column c : columns) {
			Column c2 = i.next();
			if (c.getClass() != c2.getClass())
				throw new ParseException(
						String.format(
								"Error creating foreign key for table %s: its field "
										+ "types do not coincide with field types of PK of table '%s'",
								parentTable.getName(),
								referencedTable.getName()));
			if (c2 instanceof StringColumn) {
				if (((StringColumn) c2).getLength() != ((StringColumn) c)
						.getLength()) {
					throw new ParseException(
							String.format(
									"Error creating foreign key for table %s: its string "
											+ "field length do not coincide with field length of PK of table '%s'",
									parentTable.getName(),
									referencedTable.getName()));
				}
			}
		}

		// Добавление ключа к родительской таблице (с проверкой того факта, что
		// ключа с таким же набором полей не существует).
		parentTable.addFK(this);

	}

	@Override
	public int hashCode() {
		int result = 0;
		for (Column c : columns) {
			Integer.rotateLeft(result, 3);
			result ^= c.getName().hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ForeignKey) {
			ForeignKey fk = (ForeignKey) obj;
			if (columns.size() == fk.columns.size()) {
				Iterator<Column> i = fk.columns.iterator();
				for (Column c : columns) {
					Column c2 = i.next();
					if (!c.getName().equals(c2.getName()))
						return false;
				}
				return true;
			} else {
				return false;
			}
		} else {
			return super.equals(obj);
		}
	}

	/**
	 * Добавляет колонку, на которую имеется ссылка. Список этих колонок не
	 * хранится в Foreign Key, т. к. достаточно знания имени таблицы и знания о
	 * первичном ключе таблицы (ссылки на UNIQUE-комбинации не применяются из-за
	 * отсутствия поддержки UNIQUE-комбинаций). Механизм необходим для контроля
	 * ссылочной корректности текста.
	 * 
	 * @param columnName
	 *            имя колонки
	 * @throws ParseException
	 *             Если колонка не содержится в таблице, на которую ссылаются.
	 */
	void addReferencedColumn(String columnName) throws ParseException {
		// Запускать этот метод можно только после простановки таблицы, на
		// которую ссылаемся.
		if (referencedTable == null)
			throw new IllegalStateException();
		Column c = referencedTable.getColumns().get(columnName);
		if (c == null)
			throw new ParseException(
					String.format(
							"Error creating foreign key for table '%s': column '%s' is not defined in table '%s'",
							parentTable.getName(), columnName,
							referencedTable.getName()

					));
		referencedColumns.add(c);

	}

	/**
	 * Финализирует перечень полей, на который ссылается FK. Для удобства
	 * тестирования и экономии памяти внутренний список ссылок подчищается сразу
	 * за финализацией, он нигде не хранится и нигде не доступен. Его
	 * единственная роль -- проверять правильность текста.
	 * 
	 * @throws ParseException
	 *             Если перечень полей не совпадает с перечнем полей первичного
	 *             ключа.
	 */
	public void finalizeReference() throws ParseException {

		if (referencedTable == null)
			throw new IllegalStateException();
		Map<String, Column> pk = referencedTable.getPrimaryKey();
		int size = referencedColumns.size();
		if (pk.size() != size) {
			referencedColumns.clear();
			throw new ParseException(String.format(
					"Error creating foreign key for table '%s': primary key "
							+ "length in table '%s' is %d, but the number of "
							+ "reference fields is %d.", parentTable.getName(),
					referencedTable.getName(), pk.size(), size));
		}
		Iterator<Column> i = pk.values().iterator();
		for (Column c : referencedColumns) {
			Column c2 = i.next();
			if (!c.getName().equals(c2.getName())) {
				referencedColumns.clear();
				throw new ParseException(String.format(
						"Error creating foreign key for table '%s': expected primary key "
								+ "field '%s'.'%s', but was '%s'.",
						parentTable.getName(), referencedTable.getName(),
						c2.getName(), c.getName()));
			}
		}
		referencedColumns.clear();
	}

	/**
	 * Возвращает имя ограничения FK (или null, если оно не задано).
	 */
	public String getConstraintName() {
		return constraintName;
	}

	void setConstraintName(String constraintName) {
		this.constraintName = constraintName;
	}
}