package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.*;

/**
 * Данные о колонке в базе данных в виде, необходимом для Celesta.
 */
public final class DBColumnInfo {
	private String name;
	private Class<? extends Column> type;
	private boolean isNullable;
	private String defaultValue = "";
	private int length;
	private boolean isMax;
	private boolean isIdentity;

	public String getName() {
		return name;
	}

	public Class<? extends Column> getType() {
		return type;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public int getLength() {
		return length;
	}

	public boolean isMax() {
		return isMax;
	}

	public boolean isIdentity() {
		return isIdentity;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(Class<? extends Column> type) {
		this.type = type;
	}

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public void setMax(boolean isMax) {
		this.isMax = isMax;
	}

	public void setIdentity(boolean isIdentity) {
		this.isIdentity = isIdentity;
	}

	public boolean reflects(Column value) {
		// Если тип не совпадает -- дальше не проверяем.
		if (value.getClass() != type)
			return false;

		// Проверяем nullability, но помним о том, что в Oracle DEFAULT
		// ''-строки всегда nullable
		if (type != StringColumn.class || !"''".equals(defaultValue)) {
			if (value.isNullable() != isNullable)
				return false;
		}

		if (type == IntegerColumn.class) {
			// Если свойство IDENTITY не совпадает -- не проверяем
			if (isIdentity != ((IntegerColumn) value).isIdentity())
				return false;
		} else if (type == StringColumn.class) {
			// Если параметры длин не совпали -- не проверяем
			StringColumn col = (StringColumn) value;
			if (!(isMax ? col.isMax() : length == col.getLength()))
				return false;
		}

		// Если в данных пустой default, а в метаданных -- не пустой -- то
		// не проверяем
		if (defaultValue.isEmpty()) {
			if (type == DateTimeColumn.class) {
				//do not forget DateTime's special case
				DateTimeColumn dtc = (DateTimeColumn) value;
				return dtc.getDefaultValue() == null && dtc.isGetdate() == false;
			} else {
				return value.getDefaultValue() == null;
			}
		}
		// Случай непустого default-значения в данных.
		return checkDefault(value);
	}

	private boolean checkDefault(Column value) {
		boolean result;
		if (type == BooleanColumn.class) {
			try {
				result = BooleanColumn.parseSQLBool(defaultValue).equals(value.getDefaultValue());
			} catch (ParseException e) {
				result = false;
			}
		} else if (type == IntegerColumn.class) {
			result = Integer.valueOf(defaultValue).equals(value.getDefaultValue());
		} else if (type == FloatingColumn.class) {
			result = Double.valueOf(defaultValue).equals(value.getDefaultValue());
		} else if (type == DateTimeColumn.class) {
			if ("GETDATE()".equalsIgnoreCase(defaultValue))
				result = ((DateTimeColumn) value).isGetdate();
			else {
				try {
					result = DateTimeColumn.parseISODate(defaultValue).equals(value.getDefaultValue());
				} catch (ParseException e) {
					result = false;
				}
			}
		} else if (type == StringColumn.class) {
			result = defaultValue.equals(StringColumn.quoteString(((StringColumn) value).getDefaultValue()));
		} else {
			result = defaultValue.equals(value.getDefaultValue());
		}
		return result;
	}
}
