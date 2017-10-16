package ru.curs.celesta.dbutils.filter;


/**
 * Фильтр в виде единичного значения.
 */
public class SingleValue extends AbstractFilter {
	private Object value;

	public SingleValue(Object value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.format("%s", value);
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public boolean filterEquals(AbstractFilter f) {
		if (f instanceof SingleValue) {
			Object v2 = ((SingleValue) f).value;
			return value == null ? v2 == null : value.equals(v2);
		} else {
			return false;
		}
	}
}
