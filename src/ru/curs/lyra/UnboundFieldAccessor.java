package ru.curs.lyra;

import java.sql.Date;

import org.python.core.PyObject;

/**
 * Field accessor for unbound field.
 */
public abstract class UnboundFieldAccessor implements FieldAccessor {

	@Override
	public Object getValue(Object[] c) {
		PyObject value = _getValue();
		LyraFieldType lft = LyraFieldType.valueOf(_getCelestaType());
		switch (lft) {
		case BIT:
			Boolean b = (Boolean) value.__tojava__(Boolean.class);
			return b;
		case DATETIME:
			Date d = (Date) value.__tojava__(Date.class);
			return d;
		case REAL:
			Double dbl = (Double) value.__tojava__(Double.class);
			return dbl;
		case INT:
			Integer i = (Integer) value.__tojava__(Integer.class);
			return i;
		case VARCHAR:
			String s = (String) value.__tojava__(String.class);
			return s;
		default:
			return null;
		}
	}

	/**
	 * Gets value.
	 */
	protected abstract PyObject _getValue();

	/**
	 * Gets Celesta type.
	 */
	protected abstract String _getCelestaType();
}
