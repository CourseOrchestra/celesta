package ru.curs.lyra;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.score.GrainElement;

/**
 * Base Java class for Lyra forms. Two classes inherited from this one are
 * BasicCardForm and BasicGridForm.
 */
public abstract class BasicLyraForm {

	private final GrainElement meta;
	private final LinkedHashMap<String, LyraFormField> fieldsMeta = new LinkedHashMap<>();
	private final Map<String, LyraFormField> unmodifiableMeta = Collections.unmodifiableMap(fieldsMeta);

	private BasicCursor rec;
	private CallContext context;

	BasicLyraForm(CallContext context) throws CelestaException {
		this.context = context;
		rec = _getCursor(context);
		meta = rec.meta();
		// TODO: fill fieldsMeta here
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
		return unmodifiableMeta;
	}

	/**
	 * Retrieves cursor's record metainformation.
	 */
	protected GrainElement meta() {
		return meta;
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
	// CHECKSTYLE:ON
}