package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import ru.curs.celesta.CelestaException;

/**
 * Базовый класс описания столбца таблицы. Наследники этого класса соответствуют
 * разным типам столбцов.
 */
public abstract class Column extends NamedElement implements ColumnMeta {

	private static final String OPTION = "option";
	private final TableElement parentTable;
	private boolean nullable = true;

	Column(TableElement parentTable, String name) throws ParseException {
		super(name);
		if (VersionedElement.REC_VERSION.equals(name))
			throw new ParseException(String.format("Column name '%s' is reserved for system needs.", VersionedElement.REC_VERSION));
		if (parentTable == null)
			throw new IllegalArgumentException();
		this.parentTable = parentTable;
		parentTable.addColumn(this);
	}

	/**
	 * Специальная версия конструктора для того, чтобы сконструировать поле
	 * recversion.
	 * 
	 * @param parentTable
	 *            Родительская таблица (не добавляется в перечень колонок)
	 * @throws ParseException
	 *             Не должно возникать.
	 */
	Column(TableElement parentTable) throws ParseException {
		super(VersionedElement.REC_VERSION);
		if (parentTable == null)
			throw new IllegalArgumentException();
		this.parentTable = parentTable;
		nullable = false;
		setDefault("1");
	}

	/**
	 * Возвращает опции (значение свойства option) для данного поля. Имеет
	 * применение только для текстовых и Integer-полей.
	 * 
	 * @throws CelestaException
	 *             в случае, если опции заданы неверно.
	 */
	public String[] getOptions() throws CelestaException {
		String json = getCelestaDocJSON();
		try {
			JSONObject metadata = new JSONObject(json);
			if (metadata.has(OPTION)) {
				JSONArray options = metadata.getJSONArray(OPTION);
				String[] result = new String[options.length()];
				for (int i = 0; i < options.length(); i++)
					result[i] = options.getString(i);
				return result;
			} else {
				return new String[0];
			}
		} catch (JSONException e1) {
			throw new CelestaException("Error in CelestaDoc for %s.%s.%s: %s", getParentTable().getGrain().getName(),
					getParentTable().getName(), getName(), e1.getMessage());
		}
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
	public final TableElement getParentTable() {
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
	public final void setNullableAndDefault(boolean nullable, String defaultValue) throws ParseException {
		parentTable.getGrain().modify();
		String buf = defaultValue;
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
	 * Удаляет колонку.
	 * 
	 * @throws ParseException
	 *             Если удаляется составная часть первичного ключа, внешнего
	 *             ключа или индекса.
	 */
	public final void delete() throws ParseException {
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

	/**
	 * DEFAULT-значение поля в языке CelestaSQL.
	 */
	public abstract String getCelestaDefault();
}
