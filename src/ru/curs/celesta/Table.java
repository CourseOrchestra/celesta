package ru.curs.celesta;

import java.util.Map;

/**
 * Объект-таблица в метаданных.
 * 
 */
public final class Table extends NamedElement {

	Table(String name) {
		super(name);
	}

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
	private boolean pkFinalized = false;

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
		columns.addElement(column);
	}

	@Override
	public String toString() {
		return "name: " + getName() + " " + columns.toString();
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
					"Column %s is not defined in table '%s'.", name, getName()));
		if (c.isNullable())
			throw new ParseException(String.format(
					"Column %s is nullable and therefore it cannot be "
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

	public class ForeignKey {

		private Table referencedTable;
		private FKBehaviour deleteBehaviour;
		private FKBehaviour updateBehaviour;

		private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
			@Override
			String getErrorMsg(String name) {
				return String
						.format("Column '%s' defined more than once in foreign key for table '%s'.",
								name, getName());
			}
		};

		void setReferencedTable(Table referencedTable) {
			this.referencedTable = referencedTable;
		}

		void setDeleteBehaviour(FKBehaviour deleteBehaviour) {
			this.deleteBehaviour = deleteBehaviour;
		}

		void setUpdateBehaviour(FKBehaviour updateBehaviour) {
			this.updateBehaviour = updateBehaviour;
		}

		/**
		 * Неизменяемый перечень столбцов внешнего ключа.
		 */
		public Map<String, Column> getColumns() {
			return columns.getElements();
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

	}

}
