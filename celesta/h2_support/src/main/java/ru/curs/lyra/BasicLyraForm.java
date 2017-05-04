package ru.curs.lyra;

import static ru.curs.lyra.LyraFormField.*;

import java.util.Map;
import java.util.Map.Entry;

import org.json.*;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.score.*;

/**
 * Base Java class for Lyra forms. Two classes inherited from this one are
 * BasicCardForm and BasicGridForm.
 */
public abstract class BasicLyraForm {
	private final GrainElement meta;
	private final LyraNamedElementHolder<LyraFormField> fieldsMeta = new LyraNamedElementHolder<LyraFormField>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected String getErrorMsg(String name) {
			return String.format("Field '%s' defined more than once in a form.", name);
		}
	};

	private BasicCursor rec;
	private CallContext context;

	public BasicLyraForm(CallContext context) throws CelestaException {

		_createUnboundField(fieldsMeta, "_properties_");

		this.context = context;
		rec = _getCursor(context);
		rec.navigate("-");
		meta = rec.meta();
	}

	/**
	 * A constructor for unit tests purposes only!
	 */
	BasicLyraForm(GrainElement m) throws CelestaException {
		meta = m;
	}

	/**
	 * Adds all bound fields to meta information using their CelestaDoc.
	 * 
	 * @throws CelestaException
	 *             JSON Error
	 */
	public void createAllBoundFields() throws CelestaException {
		int i = 0;
		for (Entry<String, ? extends ColumnMeta> e : meta.getColumns().entrySet()) {
			createBoundField(e.getKey(), i++, e.getValue());
		}
	}

	/**
	 * Adds all unbound fields to meta information using their decorators'
	 * parameters.
	 */
	public void createAllUnboundFields() {
		_createAllUnboundFields(fieldsMeta);
	}

	private static boolean getPropertyVal(JSONObject metadata, String propName, boolean def) throws JSONException {
		return metadata.has(propName) ? metadata.getBoolean(propName) : def;
	}

	private LyraFormField createBoundField(String name, int index, ColumnMeta m) throws CelestaException {
		LyraFieldType lft = LyraFieldType.lookupFieldType(m);
		FieldAccessor a = FieldAccessorFactory.create(index, name, lft);
		LyraFormField f = new LyraFormField(name, a);
		fieldsMeta.addElement(f);
		f.setType(LyraFieldType.lookupFieldType(m));
		String json = m.getCelestaDocJSON();
		try {
			JSONObject metadata = new JSONObject(json);
			f.setCaption(metadata.has(CAPTION) ? metadata.getString(CAPTION) : f.getName());
			f.setEditable(getPropertyVal(metadata, EDITABLE, true));
			f.setVisible(getPropertyVal(metadata, VISIBLE, true));
			if (metadata.has(SCALE)) {
				f.setScale(metadata.getInt(SCALE));
			} else {
				if (m instanceof StringColumn && !((StringColumn) m).isMax()) {
					StringColumn sc = (StringColumn) m;
					f.setScale(sc.getLength());
				} else if (m instanceof FloatingColumn) {
					// Default for floating!
					f.setScale(2);
				} else {
					f.setScale(LyraFormField.DEFAULT_SCALE);
				}
			}

			if (m instanceof Column) {
				boolean dbRequired = !((Column) m).isNullable();
				f.setRequired(metadata.has(REQUIRED) ? metadata.getBoolean(REQUIRED) | dbRequired : dbRequired);
			} else {
				f.setRequired(getPropertyVal(metadata, REQUIRED, false));
			}

			f.setWidth(metadata.has(WIDTH) ? metadata.getInt(WIDTH) : -1);

			f.setSubtype(metadata.has(SUBTYPE) ? metadata.getString(SUBTYPE) : null);
			f.setLinkId(metadata.has(LINKID) ? metadata.getString(LINKID) : null);
		} catch (JSONException e1) {
			throw new CelestaException("JSON Error: %s", e1.getMessage());
		}
		return f;
	}

	/**
	 * Adds a specific field.
	 * 
	 * @param name
	 *            Name of a table column.
	 * @throws CelestaException
	 *             JSON error in CelestaDoc.
	 */
	public LyraFormField createField(String name) throws CelestaException {
		ColumnMeta m = meta.getColumns().get(name);
		if (m == null) {
			// UNBOUND FIELD
			LyraFormField result = _createUnboundField(fieldsMeta, name);
			if (result == null)
				throw new CelestaException(String.format("Column '%s' not found in '%s.%s'", name,
						meta.getGrain().getName(), meta.getName()));
			return result;
		} else {
			// BOUND FIELD
			// finding out field's index
			int index = 0;
			for (String n : meta.getColumns().keySet()) {
				if (n.equals(name))
					break;
				index++;
			}
			return createBoundField(name, index, m);
		}
	}

	/**
	 * Sets call context for current form.
	 * 
	 * @param context
	 *            new call context.
	 */
	public synchronized void setCallContext(CallContext context) {
		this.context = context;
	}

	/**
	 * Gets current alive cursor.
	 * 
	 * @throws CelestaException
	 *             navigation error.
	 */
	// NB: never make this public, since we don't always have a correct
	// CallContext here!
	protected synchronized BasicCursor rec() throws CelestaException {
		if (rec == null) {
			if (context != null) {
				rec = _getCursor(context);
				rec.navigate("-");
			}
		} else {
			if (rec.isClosed()) {
				BasicCursor rec2 = _getCursor(context);
				rec2.copyFieldsFrom(rec);
				rec = rec2;
				rec.navigate("=>+");
			}
		}
		return rec;
	}

	protected Cursor getCursor() throws CelestaException {
		rec = rec();
		if (rec instanceof Cursor) {
			return (Cursor) rec;
		} else {
			throw new CelestaException("Cursor %s is not modifiable.", rec.meta().getName());
		}
	}

	/**
	 * Returns form fields metadata.
	 */
	public Map<String, LyraFormField> getFieldsMeta() {
		return fieldsMeta.getElements();
	}

	/**
	 * Retrieves cursor's record metainformation.
	 */
	public GrainElement meta() {
		return meta;
	}

	/**
	 * Returns column names that are in sorting.
	 * 
	 * @throws CelestaException
	 *             cannot normally occur.
	 */
	public String[] orderByColumnNames() throws CelestaException {
		return rec == null ? null : rec.orderByColumnNames();
	}

	/**
	 * Returns mask of DESC orders.
	 * 
	 * @throws CelestaException
	 *             cannot normally occur.
	 */
	public boolean[] descOrders() throws CelestaException {
		return rec == null ? null : rec.descOrders();
	}

	/*
	 * These methods are named in Python style, not Java style. This is why
	 * methods meant to be protected are called starting from underscore.
	 */
	// CHECKSTYLE:OFF
	/**
	 * Should return an active filtered and sorted cursor.
	 */
	public abstract BasicCursor _getCursor(CallContext context);

	/**
	 * Should return the form's fully qualified Python class name.
	 */
	public abstract String _getId();

	public abstract LyraFormProperties getFormProperties();

	/**
	 * Should append unbound field's meta information.
	 * 
	 * @param meta
	 *            Editable meta (NB: getFieldsMeta() returns read-only meta).
	 * @param name
	 *            Name of the field to be appended to form.
	 */
	protected abstract LyraFormField _createUnboundField(LyraNamedElementHolder<LyraFormField> meta, String name);

	/**
	 * Should create all unbound fields
	 * 
	 * @param fieldsMeta
	 *            Editable meta (NB: getFieldsMeta() returns read-only meta).
	 */
	protected abstract void _createAllUnboundFields(LyraNamedElementHolder<LyraFormField> fieldsMeta);

	public abstract void _beforeSending(BasicCursor c);
	// CHECKSTYLE:ON

	CallContext getContext() {
		return context;
	}
}