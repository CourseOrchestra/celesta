package ru.curs.celesta;

/**
 * Базовый класс описания столбца таблицы. Наследники этого класса соответствуют
 * разным типам столбцов.
 */
public abstract class Column extends NamedElement {

	private final Table parentTable;
	private boolean nullable = true;

	Column(Table parentTable, String name) throws ParseException {
		super(name);
		if (parentTable == null)
			throw new IllegalArgumentException();
		this.parentTable = parentTable;
		parentTable.addColumn(this);
	}

	protected abstract void setDefault(String lexvalue) throws ParseException;

	@Override
	public String toString() {
		return getName();
	}

	/**
	 * Возвращает таблицу, к которой относится данная колонка.
	 */
	public final Table getParentTable() {
		return parentTable;
	}

	/**
	 * Устанавливает свойство nullable и значение по умолчанию.
	 * 
	 * @param nullable
	 *            свойство Nullable
	 * @param defaultValue
	 *            значение по умолчанию
	 * @throws ParseException
	 *             в случае, если NOT NULL идёт без DEFAULT
	 */
	public final void setNullableAndDefault(boolean nullable,
			String defaultValue) throws ParseException {
		if (defaultValue == null && !nullable)
			throw new ParseException(
					String.format(
							"Column %s is defined as NOT NULL but has no default value",
							getName()));
		this.nullable = nullable;
		setDefault(defaultValue);
	}

	/**
	 * Возвращает значение свойства Nullable.
	 */
	public final boolean isNullable() {
		return nullable;
	}
}
