package ru.curs.lyra;

import java.sql.Date;

import org.python.core.Py;
import org.python.core.PyObject;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;

/**
 * Field accessor for unbound field.
 */
public final class UnboundFieldAccessor implements FieldAccessor {
	private final PyObject getter;
	private final PyObject setter;
	private final LyraFieldType lft;

	public UnboundFieldAccessor(String celestaType, PyObject getter, PyObject setter) {
		this.getter = getter;
		this.setter = setter;
		this.lft = LyraFieldType.valueOf(celestaType);
	}

	@Override
	public Object getValue(Object[] c) {
		PyObject value = getter.__call__();
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
		setter.__call__(p);
	}
}
