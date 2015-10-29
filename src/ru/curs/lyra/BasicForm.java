package ru.curs.lyra;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;

public abstract class BasicForm {

	protected BasicCursor rec;

	public BasicForm() {
		super();
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
			throw new CelestaException("Cursor %s is not modifiable.", rec
					.meta().getName());
		}
	}

	public abstract BasicCursor _getCursor();

	public abstract String _getId();

}