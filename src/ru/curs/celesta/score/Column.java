package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

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

	/**
	 * Устанаавливает значение по умолчанию.
	 * 
	 * @param lexvalue
	 *            Новое значение в строковом (лексическом) формате.
	 * @throws ParseException
	 *             Если формат значения по умолчанию неверен.
	 */
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
	 *             в случае, если значение DEFAULT имеет неверный формат.
	 */
	public final void setNullableAndDefault(boolean nullable,
			String defaultValue) throws ParseException {
		parentTable.getGrain().modify();
		String buf;
		// if (defaultValue == null && !nullable) {
		// buf = getDefaultDefault();
		// } else {
		buf = defaultValue;
		// }
		this.nullable = nullable;
		setDefault(buf);
	}

	/**
	 * Возвращает значение свойства Nullable.
	 */
	public final boolean isNullable() {
		return nullable;
	}

	/**
	 * Пустое значение в языке Python. Необходимо для процедур генерации
	 * ORM-кода.
	 */
	public abstract String pythonDefaultValue();

	/**
	 * Имя JDBC-геттера, подходящего для данного типа колонки. Необходимо для
	 * процедур генерации ORM-кода.
	 */
	public abstract String jdbcGetterName();

	/**
	 * Удаляет колонку.
	 * 
	 * @throws ParseException
	 *             Если удаляется составная часть первичного ключа, внешнего
	 *             ключа или индекса.
	 */
	final void delete() throws ParseException {
		parentTable.removeColumn(this);
	}

	void save(BufferedWriter bw) throws IOException {
		bw.write("  ");
		if (Grain.writeCelestaDoc(this, bw))
			bw.write("  ");
		bw.write(getName());
	}

	/**
	 * Возвращает значение по умолчанию.
	 */
	public abstract Object getDefaultValue();
}
