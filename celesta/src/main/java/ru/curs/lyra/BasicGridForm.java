package ru.curs.lyra;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.GridDriver;
import ru.curs.celesta.score.ParseException;

/**
 * Base Java class for Lyra grid form.
 */
public abstract class BasicGridForm extends BasicLyraForm {

	private GridDriver gd;

	private LinkedList<BasicCursor> savedPositions = new LinkedList<>();

	public BasicGridForm(CallContext context) throws CelestaException {
		super(context);
		actuateGridDriver(_getCursor(context));
	}

	private void actuateGridDriver(BasicCursor c) throws CelestaException {
		if (gd == null) {
			gd = new GridDriver(c);
		} else if (!gd.isValidFor(c)) {
			Runnable notifier = gd.getChangeNotifier();
			gd = new GridDriver(c);
			gd.setChangeNotifier(notifier);
			gd.setMaxExactScrollValue(gd.getMaxExactScrollValue());
		}
	}

	private <T> T reconnect(Callable<T> f, T fallBack) throws CelestaException {
		CallContext context = getContext();
		if (context == null)
			return fallBack;
		boolean closeContext = context.isClosed();
		Connection conn = null;
		if (closeContext) {
			conn = ConnectionPool.get();
			setCallContext(context.getCopy(conn));
		}
		try {
			return f.call();
		} catch (CelestaException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new CelestaException("Error %s while retrieving grid rows: %s", e.getClass().getName(),
					e.getMessage());
		} finally {
			if (closeContext) {
				getContext().closeCursors();
				ConnectionPool.putBack(conn);
			}
		}
	}

	public synchronized List<LyraFormData> getRows(int position) throws CelestaException {
		return getRowsH(position, getGridHeight());
	}

	/**
	 * Returns contents of grid given scrollbar's position.
	 * 
	 * @param position
	 *            New scrollbar's position.
	 * @throws CelestaException
	 *             e. g. insufficient access rights
	 */
	public synchronized List<LyraFormData> getRowsH(int position, int h) throws CelestaException {
		return reconnect(() -> {

			BasicCursor c = rec();
			actuateGridDriver(c);
			if (gd.setPosition(position, c)) {
				return returnRows(c, h);
			} else {
				return Collections.emptyList();
			}
		} , Collections.emptyList());
	}

	public synchronized List<LyraFormData> getRows() throws CelestaException {
		return getRowsH(getGridHeight());
	}

	/**
	 * Returns contents of grid for current cursor's position.
	 * 
	 * @throws CelestaException
	 *             e. g. insufficient user rights.
	 */
	public synchronized List<LyraFormData> getRowsH(int h) throws CelestaException {
		return reconnect(() -> {
			BasicCursor bc = rec();
			// TODO: optimize for reducing DB SELECT calls!
			if (bc.navigate("=<-")) {
				gd.setPosition(bc);
				return returnRows(bc, h);
			} else {
				return Collections.emptyList();
			}
		} , Collections.emptyList());
	}

	public synchronized List<LyraFormData> setPosition(Object... pk) throws CelestaException {
		return setPositionH(getGridHeight(), pk);
	}

	/**
	 * Positions grid to a certain record.
	 * 
	 * @param pk
	 *            Values of primary key.
	 * 
	 * @throws CelestaException
	 *             e. g. insufficient access rights
	 */
	public synchronized List<LyraFormData> setPositionH(int h, Object... pk) throws CelestaException {
		return reconnect(() -> {
			BasicCursor bc = rec();
			actuateGridDriver(bc);

			if (bc instanceof Cursor) {
				Cursor c = (Cursor) bc;
				if (c.meta().getPrimaryKey().size() != pk.length)
					throw new CelestaException(
							"Invalid number of 'setPosition' arguments for '%s': expected %d, provided %d.",
							c.meta().getName(), c.meta().getPrimaryKey().size(), pk.length);
				int i = 0;
				for (String name : c.meta().getPrimaryKey().keySet()) {
					c.setValue(name, pk[i++]);
				}
			} else {
				bc.setValue(bc.meta().getColumns().keySet().iterator().next(), pk[0]);
			}

			if (bc.navigate("=<-")) {
				gd.setPosition(bc);
				return returnRows(bc, h);
			} else {
				return Collections.emptyList();
			}

		} , Collections.emptyList());

	}

	private List<LyraFormData> returnRows(BasicCursor c, int h) throws CelestaException {

		final String id = _getId();
		final List<LyraFormData> result = new ArrayList<>(h);
		final Map<String, LyraFormField> meta = getFieldsMeta();
		BasicCursor copy = c._getBufferCopy(c.callContext());
		copy.close();
		for (int i = 0; i < h; i++) {
			_beforeSending(c);
			LyraFormData lfd = new LyraFormData(c, meta, id);
			result.add(lfd);
			if (!c.next())
				break;
		}
		// return to the beginning!
		c.copyFieldsFrom(copy);

		if (result.size() < h) {
			for (int i = result.size(); i < h; i++) {
				if (!c.previous())
					break;
				_beforeSending(c);
				LyraFormData lfd = new LyraFormData(c, meta, id);
				result.add(0, lfd);

			}
			c.copyFieldsFrom(copy);
		}
		return result;
	}

	/**
	 * Sets change notifier to be run when refined grid parameters are ready.
	 * 
	 * @param callback
	 *            A callback to be run.
	 */
	public void setChangeNotifier(Runnable callback) {
		gd.setChangeNotifier(callback);
	}

	/**
	 * Returns change notifier.
	 */
	public Runnable getChangeNotifier() {
		return gd.getChangeNotifier();
	}

	/**
	 * If the grid is scrolled less than for given amount of records, the exact
	 * positioning in cycle will be used instead of interpolation.
	 * 
	 * @param val
	 *            new value.
	 */
	public void setMaxExactScrollValue(int val) {
		gd.setMaxExactScrollValue(val);
	}

	/**
	 * Returns (approximate) total record count.
	 * 
	 * Just after creation of the form this method returns DEFAULT_COUNT value,
	 * but it asynchronously requests total count right after constructor
	 * execution.
	 */
	public int getApproxTotalCount() {
		return gd.getApproxTotalCount();

	}

	/**
	 * Returns scrollbar's knob position for current cursor value.
	 */
	public int getTopVisiblePosition() {
		return gd.getTopVisiblePosition();
	}

	public void saveCursorPosition() throws CelestaException {
		reconnect(() -> {
			BasicCursor copy = rec()._getBufferCopy(getContext());
			copy.close();
			savedPositions.push(copy);
			return null;
		} , null);
	}

	public void restoreCursorPosition() throws CelestaException {
		reconnect(() -> {
			BasicCursor copy = savedPositions.pop();
			rec().copyFieldsFrom(copy);
			return null;
		} , null);
	}

	/**
	 * Should return a number of rows in grid.
	 */
	public abstract int getGridHeight();
}
