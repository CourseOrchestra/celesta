package ru.curs.lyra;

import java.sql.Date;

import org.python.core.*;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;

/**
 * Field accessor for unbound field.
 */
public final class UnboundFieldAccessor implements FieldAccessor {
	private final PyObject getter;
	private final PyObject setter;
	private final LyraFieldType lft;
	private final PyObject instance;

	public UnboundFieldAccessor(String celestaType, PyObject getter, PyObject setter,
			PyObject instance) {
		this.getter = getter;
		this.setter = setter;
		this.lft = LyraFieldType.valueOf(celestaType);
		this.instance = instance;
	}

	@Override
	public Object getValue(Object[] c) throws CelestaException {

		PyObject value = null;
		try {
			value = getter.__call__(instance);
		} catch (PyException e) {
			e.printStackTrace();
			throw new CelestaException(
					"Python error while getting unbound field value: %s. See logs for details.",
					e.value == null ? "null" : e.value.toString());
		} catch (Throwable e) {
			e.printStackTrace();
			throw new CelestaException(
					"Error %s while getting unbound field value: %s. See logs for details.",
					e.getClass().getName(), e.getMessage());
		}

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

	@Override
	public void setValue(BasicCursor c, Object newValue) throws CelestaException {
		PyObject p = Py.java2py(newValue);
		setter.__call__(instance, p);
	}
}
