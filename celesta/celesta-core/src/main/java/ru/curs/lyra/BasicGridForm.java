package ru.curs.lyra;

import java.sql.Connection;
import java.util.*;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.*;

/**
 * Base Java class for Lyra grid form.
 */
public abstract class BasicGridForm extends BasicLyraForm {

	@FunctionalInterface
	public interface ExternalAction<T> {
		T call(BasicCursor t) throws CelestaException;
	}

	private GridDriver gd;

	private final LinkedList<BasicCursor> savedPositions = new LinkedList<>();

	public BasicGridForm(CallContext context) throws CelestaException {
		super(context);
		actuateGridDriver(_getCursor(context));
	}

	private void actuateGridDriver(BasicCursor c) throws CelestaException {
		if (gd == null) {
			gd = new GridDriver(c);
		} else if (!gd.isValidFor(c)) {
			Runnable notifier = gd.getChangeNotifier();
			int maxExactScrollValue = gd.getMaxExactScrollValue();
			gd = new GridDriver(c);
			gd.setChangeNotifier(notifier);
			gd.setMaxExactScrollValue(maxExactScrollValue);
		}
	}

	public <T> T externalAction(ExternalAction<T> f) throws CelestaException {
		return externalAction(f, null);
	}

	public <T> T externalAction(ExternalAction<T> f, T fallBack) throws CelestaException {
		CallContext context = getContext();
		if (context == null)
			return fallBack;
		boolean closeContext = context.isClosed();
		if (closeContext) {
			setCallContext(context.getCopy());
		}
		try {
			return f.call(rec());
		} catch (CelestaException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new CelestaException("Error %s while retrieving grid rows: %s",
					e.getClass().getName(), e.getMessage());
		} finally {
			if (closeContext) {
				getContext().close();
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
		return externalAction(c -> {
			actuateGridDriver(c);
			if (gd.setPosition(position, c)) {
				return returnRows(c, h);
			} else {
				return Collections.emptyList();
			}
		}, Collections.emptyList());
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
		return externalAction(bc -> {
			// TODO: optimize for reducing DB SELECT calls!
			if (bc.navigate("=<-")) {
				gd.setPosition(bc);
				return returnRows(bc, h);
			} else {
				return Collections.emptyList();
			}
		}, Collections.emptyList());
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
	public synchronized List<LyraFormData> setPositionH(int h, Object... pk)
			throws CelestaException {
		return externalAction(bc -> {
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

		}, Collections.emptyList());

	}

	private List<LyraFormData> returnRows(BasicCursor c, int h) throws CelestaException {

		final String id = _getId();
		final List<LyraFormData> result = new ArrayList<>(h);
		final Map<String, LyraFormField> meta = getFieldsMeta();
		BasicCursor copy = c._getBufferCopy(c.callContext(), null);
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
		externalAction(c -> {
			BasicCursor copy = c._getBufferCopy(getContext(), null);
			copy.close();
			savedPositions.push(copy);
			return null;
		}, null);
	}

	public void restoreCursorPosition() throws CelestaException {
		externalAction(c -> {
			BasicCursor copy = savedPositions.pop();
			rec().copyFieldsFrom(copy);
			return null;
		}, null);
	}

	/**
	 * Should return a number of rows in grid.
	 */
	public abstract int getGridHeight();
}
