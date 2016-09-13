package ru.curs.lyra;

import java.text.SimpleDateFormat;
import java.util.Date;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;

/**
 * Abstract Lyra form field getter/setter.
 */
public interface FieldAccessor {
	/**
	 * Get field's value.
	 * 
	 * @param c
	 *            Cursor values (ignored for unbound field).
	 * @throws CelestaException
	 *             Error getting the value.
	 */
	Object getValue(Object[] c) throws CelestaException;

	/**
	 * Set field's value.
	 * 
	 * @param c
	 *            Cursor (ignored for unbound field).
	 * @param newValue
	 *            New field's value.
	 * @throws CelestaException
	 *             unsuccessful setting.
	 */
	void setValue(BasicCursor c, Object newValue) throws CelestaException;

}

/**
 * FieldAccessor factory: instantiates an appropriate FieldAccessor for bound
 * field given a ColumnMeta.
 */
final class FieldAccessorFactory {

	/**
	 * Base class for bound field accessor implementations.
	 */
	private abstract static class BasicBoundFieldAccessor implements FieldAccessor {
		private final int index;
		private final String name;

		BasicBoundFieldAccessor(int index, String name) {
			this.index = index;
			this.name = name;
		}

		@Override
		public final Object getValue(Object[] c) {
			return c[index];
		}

		@Override
		public final void setValue(BasicCursor c, Object newValue) throws CelestaException {
			if (newValue == null) {
				c.setValue(name, null);
			} else {
				String buf = newValue.toString();
				setValue(c, newValue, buf);
			}
		}

		final String name() {
			return name;
		}

		abstract void setValue(BasicCursor c, Object newValue, String buf) throws CelestaException;
	}

	private FieldAccessorFactory() {

	}

	static FieldAccessor create(int index, String name, LyraFieldType lft) {

		switch (lft) {
		case DATETIME:
			return new BasicBoundFieldAccessor(index, name) {
				private SimpleDateFormat sdf;

				@Override
				public void setValue(BasicCursor c, Object val, String buf) throws CelestaException {
					if (val instanceof Date)
						c.setValue(name(), val);
					else {
						if (sdf == null)
							sdf = new SimpleDateFormat(LyraFieldValue.XML_DATE_FORMAT);

						Date d;
						try {
							d = sdf.parse(buf);
						} catch (java.text.ParseException e) {
							d = null;
						}
						c.setValue(name(), d);
					}
				}
			};

		case BIT:
			return new BasicBoundFieldAccessor(index, name) {
				@Override
				public void setValue(BasicCursor c, Object val, String buf) throws CelestaException {
					c.setValue(name(), Boolean.valueOf(buf));
				}
			};
		case INT:
			return new BasicBoundFieldAccessor(index, name) {
				@Override
				public void setValue(BasicCursor c, Object val, String buf) throws CelestaException {
					c.setValue(name(), Integer.valueOf(buf));
				}
			};

		case REAL:
			return new BasicBoundFieldAccessor(index, name) {
				@Override
				public void setValue(BasicCursor c, Object val, String buf) throws CelestaException {
					c.setValue(name(), Double.valueOf(buf));
				}
			};

		default:
			return new BasicBoundFieldAccessor(index, name) {
				@Override
				public void setValue(BasicCursor c, Object val, String buf) throws CelestaException {
					c.setValue(name(), buf);
				}
			};

		}

	}
}