package ru.curs.celesta.dbutils;

/**
 * Внутреннее представление фильтра на поле.
 */
abstract class AbstractFilter {

}

/**
 * Фильтр в виде единичного значения.
 */
class SingleValue extends AbstractFilter {
	private final Object value;

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
}

/**
 * Фильтр в виде диапазона значений от..до.
 * 
 */
class Range extends AbstractFilter {
	private final Object valueFrom;
	private final Object valueTo;

	public Range(Object valueFrom, Object valueTo) {
		this.valueFrom = valueFrom;
		this.valueTo = valueTo;
	}

	@Override
	public String toString() {
		return String.format("%s..%s", valueFrom, valueTo);
	}

	public Object getValueFrom() {
		return valueFrom;
	}

	public Object getValueTo() {
		return valueTo;
	}
}

/**
 * Сложный фильтр.
 */
class Filter extends AbstractFilter {
	private final String value;

	public Filter(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.format("%s", value);
	}
}