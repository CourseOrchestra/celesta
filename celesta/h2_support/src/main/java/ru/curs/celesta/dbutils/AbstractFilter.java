package ru.curs.celesta.dbutils;

import java.util.HashMap;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.FilterParser;
import ru.curs.celesta.dbutils.filter.FilterParser.FilterType;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;

/**
 * Внутреннее представление фильтра на поле.
 */
abstract class AbstractFilter {
	abstract boolean filterEquals(AbstractFilter f);
}

/**
 * Фильтр в виде единичного значения.
 */
class SingleValue extends AbstractFilter {
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

	void setValue(Object value) {
		this.value = value;
	}

	@Override
	boolean filterEquals(AbstractFilter f) {
		if (f instanceof SingleValue) {
			Object v2 = ((SingleValue) f).value;
			return value == null ? v2 == null : value.equals(v2);
		} else {
			return false;
		}
	}
}

/**
 * Фильтр в виде диапазона значений от..до.
 * 
 */
class Range extends AbstractFilter {
	private Object valueFrom;
	private Object valueTo;

	public Range(Object valueFrom, Object valueTo) {
		this.valueFrom = valueFrom;
		this.valueTo = valueTo;
	}

	@Override
	public String toString() {
		return String.format("%s..%s", valueFrom.toString(), valueTo.toString());
	}

	public Object getValueFrom() {
		return valueFrom;
	}

	public Object getValueTo() {
		return valueTo;
	}

	protected void setValues(Object valueFrom, Object valueTo) {
		this.valueFrom = valueFrom;
		this.valueTo = valueTo;
	}

	@Override
	boolean filterEquals(AbstractFilter f) {
		if (f instanceof Range) {
			Object f2 = ((Range) f).valueFrom;
			Object t2 = ((Range) f).valueTo;
			return (valueFrom == null ? f2 == null : valueFrom.equals(f2))
					&& (valueTo == null ? t2 == null : valueTo.equals(t2));
		} else {
			return false;
		}
	}
}

/**
 * Фильтр по одному полю.
 */
class Filter extends AbstractFilter {

	private static final HashMap<String, FilterType> C2F = new HashMap<>();

	static {
		C2F.put(IntegerColumn.CELESTA_TYPE, FilterType.NUMERIC);
		C2F.put(FloatingColumn.CELESTA_TYPE, FilterType.NUMERIC);
		C2F.put(DateTimeColumn.CELESTA_TYPE, FilterType.DATETIME);
		C2F.put(StringColumn.VARCHAR, FilterType.TEXT);
		C2F.put(StringColumn.TEXT, FilterType.TEXT);
	}

	private final String value;
	private final FilterType ftype;

	public Filter(String value, ColumnMeta c) {
		this.value = value;
		this.ftype = C2F.getOrDefault(c.getCelestaType(), FilterType.OTHER);
	}

	@Override
	public String toString() {
		return value;
	}

	public String makeWhereClause(String quotedName, final QueryBuildingHelper dba) throws CelestaException {

		FilterParser.SQLTranslator tr = new FilterParser.SQLTranslator() {
			@Override
			public String translateDate(String date) throws CelestaException {
				return dba.translateDate(date);
			}

		};
		String result = FilterParser.translateFilter(ftype, quotedName, value, tr);
		return result;
	}

	@Override
	boolean filterEquals(AbstractFilter f) {
		if (f instanceof Filter) {
			Object v2 = ((Filter) f).value;
			return value == null ? v2 == null : value.equals(v2);
		} else {
			return false;
		}
	}
}