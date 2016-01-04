package ru.curs.lyra;

import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.NamedElementHolder;
import ru.curs.celesta.score.ParseException;

/**
 * Base Java class for Lyra forms. Two classes inherited from this one are
 * BasicCardForm and BasicGridForm.
 */
public abstract class BasicLyraForm {

	/**
	 * Visible property name.
	 */
	public static final String VISIBLE = "visible";
	/**
	 * Editable property name.
	 */
	public static final String EDITABLE = "editable";
	/**
	 * Caption property name.
	 */
	public static final String CAPTION = "caption";
	private final GrainElement meta;
	private final NamedElementHolder<LyraFormField> fieldsMeta = new NamedElementHolder<LyraFormField>() {
		@Override
		protected String getErrorMsg(String name) {
			return String.format("Field '%s' defined more than once in a form.", name);
		}
	};

	private BasicCursor rec;
	private CallContext context;

	public BasicLyraForm(CallContext context) throws CelestaException, ParseException {
		this.context = context;
		rec = _getCursor(context);
		rec.navigate("-");
		meta = rec.meta();
		_buildUnboundFieldsMeta(fieldsMeta);
	}

	/**
	 * A constructor for unit tests purposes only!
	 */
	BasicLyraForm(GrainElement m) throws ParseException, CelestaException {
		meta = m;
		_buildUnboundFieldsMeta(fieldsMeta);
	}

	/**
	 * Adds all bound fields to meta information using their CelestaDoc.
	 * 
	 * @throws ParseException
	 *             duplicate field names
	 * @throws CelestaException
	 *             JSON Error
	 */
	public void addAllBoundFields() throws ParseException, CelestaException {
		for (Entry<String, ? extends ColumnMeta> e : meta.getColumns().entrySet()) {
			addBoundField(e.getKey(), e.getValue());
		}
	}

	private void addBoundField(String name, ColumnMeta m) throws ParseException, CelestaException {
		LyraFormField f = new LyraFormField(name);
		fieldsMeta.addElement(f);
		f.setType(LyraFieldType.lookupFieldType(m));
		String json = extractJSON(m.getCelestaDoc());
		try {
			JSONObject metadata = new JSONObject(json);
			f.setCaption(metadata.has(CAPTION) ? metadata.getString(CAPTION) : f.getName());
			f.setEditable(metadata.has(EDITABLE) ? metadata.getBoolean(EDITABLE) : true);
			f.setVisible(metadata.has(VISIBLE) ? metadata.getBoolean(VISIBLE) : true);
		} catch (JSONException e1) {
			throw new CelestaException("JSON Error: %s", e1.getMessage());
		}

	}

	/**
	 * Adds only specific bound field.
	 * 
	 * @param name
	 *            Name of a table column.
	 * @throws ParseException
	 *             No column with such name found.
	 * @throws CelestaException
	 *             JSON error in CelestaDoc.
	 */
	public void addBoundField(String name) throws ParseException, CelestaException {
		ColumnMeta m = meta.getColumns().get(name);
		if (m == null)
			throw new ParseException(
					String.format("Column '%s' not found in '%s.%s'", name, meta.getGrain().getName(), meta.getName()));
		addBoundField(name, m);
	}

	/**
	 * Sets call context for current form.
	 * 
	 * @param context
	 *            new call context.
	 */
	public void setCallContext(CallContext context) {
		this.context = context;
	}

	/**
	 * Gets current alive cursor.
	 * 
	 * @throws CelestaException
	 *             navigation error.
	 */
	public BasicCursor rec() throws CelestaException {
		if (rec == null) {
			if (context != null) {
				rec = _getCursor(context);
				rec.navigate("-");
			}
		} else {
			if (rec.callContext() != context) {
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
	protected GrainElement meta() {
		return meta;
	}

	// CHECKSTYLE:OFF for cyclomatic complexity
	static String extractJSON(String celestaDoc) throws CelestaException {
		// CHECKSTYLE:ON
		if (celestaDoc == null)
			return "{}";
		StringBuilder sb = new StringBuilder();
		int state = 0;
		int bracescount = 0;
		for (int i = 0; i < celestaDoc.length(); i++) {
			char c = celestaDoc.charAt(i);
			switch (state) {
			case 0:
				if (c == '{') {
					sb.append(c);
					bracescount++;
					state = 1;
				}
				break;
			case 1:
				sb.append(c);
				if (c == '{') {
					bracescount++;
				} else if (c == '}') {
					if (--bracescount == 0)
						return sb.toString();
				} else if (c == '"') {
					state = 2;
				}
				break;
			case 2:
				sb.append(c);
				if (c == '\\') {
					state = 3;
				} else if (c == '"') {
					state = 1;
				}
				break;
			case 3:
				sb.append(c);
				state = 2;
				break;
			default:
			}
		}
		// No valid json!
		if (state != 0)
			throw new CelestaException("Broken or truncated JSON: %s", sb.toString());
		return "{}";
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

	/**
	 * Should append unbound fields meta information.
	 * 
	 * @param meta
	 *            Editable meta (NB: getFieldsMeta() returns read-only meta).
	 */
	public abstract void _buildUnboundFieldsMeta(NamedElementHolder<LyraFormField> meta);
	// CHECKSTYLE:ON
}