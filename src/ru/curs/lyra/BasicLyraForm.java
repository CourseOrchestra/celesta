package ru.curs.lyra;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.score.GrainElement;

/**
 * Base Java class for Lyra forms. Two classes inherited from this one are
 * BasicCardForm and BasicGridForm.
 */
public abstract class BasicLyraForm {

	private BasicCursor rec;

	public BasicLyraForm() {
		super();
	}

	protected BasicCursor rec() {
		return rec;
	}

	protected boolean updateRec() throws CelestaException {
		if (rec == null) {
			rec = _getCursor();
			return true;
		} else if (rec.isClosed()) {
			BasicCursor rec2 = _getCursor();
			rec2.copyFieldsFrom(rec);
			rec = rec2;
		}
		return false;
	}

	protected Cursor getCursor() throws CelestaException {
		updateRec();
		if (rec instanceof Cursor) {
			return (Cursor) rec;
		} else {
			throw new CelestaException("Cursor %s is not modifiable.", rec.meta().getName());
		}
	}

	/**
	 * Retrieves cursor's record metainformation.
	 * 
	 * @throws CelestaException
	 *             Error while retrieving meta (should not happen normally).
	 */
	protected GrainElement getRecMeta() throws CelestaException {
		if (rec == null)
			rec = _getCursor();
		return rec.meta();
	}

	/*
	 * These methods are named in Python style, not Java style. This is why
	 * methods meant to be protected are called starting from underscore.
	 */
	// CHECKSTYLE:OFF
	/**
	 * Should return an active filtered and sorted cursor.
	 */
	public abstract BasicCursor _getCursor();

	/**
	 * Should return the form's fully qualified Python class name.
	 */
	public abstract String _getId();
	// CHECKSTYLE:ON
}