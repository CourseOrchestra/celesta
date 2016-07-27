package ru.curs.celesta.dbutils;

import java.math.BigInteger;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;
import ru.curs.lyra.grid.BitFieldEnumerator;
import ru.curs.lyra.grid.CompositeKeyEnumerator;
import ru.curs.lyra.grid.IntFieldEnumerator;
import ru.curs.lyra.grid.KeyEnumerator;
import ru.curs.lyra.grid.KeyInterpolator;
import ru.curs.lyra.grid.NullableFieldEnumerator;
import ru.curs.lyra.grid.VarcharFieldEnumerator;

/**
 * Specifies the record position asynchronously, using separate execution
 * thread.
 */
public final class GridDriver {

	private static final int MAX_REFINEMENTS_COUNT = 100;
	private static final int DEFAULT_SMALL_SCROLL = 11;

	/**
	 * The default assumption for a records count in a table.
	 */
	private static final int DEFAULT_COUNT = 1024;

	private final KeyInterpolator interpolator;
	private final KeyEnumerator rootKeyEnumerator;
	private final Map<String, KeyEnumerator> keyEnumerators = new HashMap<>();
	private final Random rnd = new Random();

	/**
	 * Key columns' names.
	 */
	// private final String[] names;

	private Runnable changeNotifier;

	private CounterThread counterThread = null;

	/**
	 * Inexact primary key for latest refinement request (cached to prevent
	 * repeated refinement of the same value).
	 */
	private BigInteger latestRequest;

	/**
	 * Exact primary key for the current top visible record.
	 */
	private BigInteger topVisiblePosition;

	private RequestTask task;

	/**
	 * A closed copy of underlying cursor that handles filters and sorting.
	 */
	private final BasicCursor closedCopy;

	private int refinementsCount = 0;

	private int smallScroll = DEFAULT_SMALL_SCROLL;

	/**
	 * Handles asynchronous interpolation table refinement requests.
	 */
	private final class CounterThread extends Thread {

		@Override
		public void run() {

			Connection conn = null;
			CallContext sysContext = null;
			try {
				conn = ConnectionPool.get();
				sysContext = new CallContext(conn, BasicCursor.SYSTEMSESSION);
				BasicCursor c = closedCopy._getBufferCopy(sysContext);
				c.copyFiltersFrom(closedCopy);
				c.copyOrderFrom(closedCopy);

				while (true) {
					RequestTask myRequest = task;
					if (myRequest == null) {
						if (refinementsCount > MAX_REFINEMENTS_COUNT)
							return;
						refinementsCount++;
						BigInteger lav = interpolator.getLeastAccurateValue();
						if (lav == null) {
							// no refinements needed at all at the moment,
							return;
						} else {
							// perform interpolation table refinement
							setCursorOrdinal(c, lav);
							if (rnd.nextBoolean()) {
								c.navigate("=>+");
							} else {
								c.navigate("=<-");
							}
							int result = c.position();
							BigInteger key = getCursorOrdinal(c);
							interpolator.setPoint(key, result);
							// if (changeNotifier != null)
							// changeNotifier.run();
							continue;
						}
					}

					// check if it's time to execute the request
					long delta = myRequest.getDelayBeforeRun();
					if (delta > 0) {
						Thread.sleep(delta);
						continue;
					}

					task = null;

					// Execute the refinement task!
					setCursorOrdinal(c, myRequest.getKey());
					int result = c.position();
					interpolator.setPoint(myRequest.getKey(), result);
					if (changeNotifier != null)
						changeNotifier.run();
				}
			} catch (CelestaException | InterruptedException e) {
				// terminate thread silently
				return;
			} finally {
				if (sysContext != null)
					sysContext.closeCursors();
				counterThread = null;
				ConnectionPool.putBack(conn);
			}
		}
	}

	public GridDriver(BasicCursor c, Runnable callback) throws CelestaException {
		this(c);
		setChangeNotifier(callback);
	}

	public GridDriver(BasicCursor c) throws CelestaException {
		closedCopy = c._getBufferCopy(c.callContext());
		closedCopy.copyFiltersFrom(c);
		closedCopy.copyOrderFrom(c);
		closedCopy.close();

		// checking order
		final boolean[] descOrders = c.descOrders();
		final boolean desc = descOrders[0];
		for (int i = 1; i < descOrders.length; i++) {
			if (desc != descOrders[i])
				throw new CelestaException("Mixed ASC/DESC ordering for grid: %s", c.getOrderBy());
		}

		// KeyEnumerator factory
		final GrainElement meta = c.meta();
		final String[] quotedNames = c.orderByColumnNames();
		final String[] names = new String[quotedNames.length];
		for (int i = 0; i < quotedNames.length; i++) {
			names[i] = quotedNames[i].substring(1, quotedNames[i].length() - 1);
		}
		if (names.length == 1) {
			// Single field key enumerator
			ColumnMeta m = meta.getColumns().get(names[0]);
			rootKeyEnumerator = createFieldKeyManager(m);
			keyEnumerators.put(names[0], rootKeyEnumerator);
		} else {
			// Multiple field key enumerator
			KeyEnumerator[] km = new KeyEnumerator[names.length];
			for (int i = 0; i < names.length; i++) {
				ColumnMeta m = meta.getColumns().get(names[i]);
				km[i] = createFieldKeyManager(m);
				keyEnumerators.put(names[i], km[i]);
			}
			rootKeyEnumerator = new CompositeKeyEnumerator(km);
		}

		if (c.navigate("+")) {
			BigInteger higherOrd = getCursorOrdinal(c);
			// Request a total record count immediately
			requestRefinement(higherOrd, true);
			c.navigate("-");
			BigInteger lowerOrd = getCursorOrdinal(c);
			interpolator = new KeyInterpolator(lowerOrd, higherOrd, DEFAULT_COUNT, desc);
			topVisiblePosition = lowerOrd;
		} else {
			// empty record set!
			interpolator = new KeyInterpolator(BigInteger.ZERO, BigInteger.ZERO, 0, desc);
			topVisiblePosition = BigInteger.ZERO;
		}

	}

	/**
	 * Fills key fields of a cursor based on scroller knob position.
	 * 
	 * @param position
	 *            scrollbar knob position
	 * @param c
	 *            Alive cursor to be modified
	 * @return false if record set is empty
	 * @throws CelestaException
	 *             e.g. wrong cursor
	 */
	public boolean setPosition(int position, BasicCursor c) throws CelestaException {
		checkMeta(c);
		// First, we are checking if exact positioning is possible
		final int closestPosition = interpolator.getClosestPosition(position);
		final int absDelta = Math.abs(position - closestPosition);

		if (absDelta < smallScroll) {
			// Trying to perform exact positioning!
			BigInteger key = interpolator.getExactPoint(closestPosition);
			if (key != null) {
				setCursorOrdinal(c, key);
				if (c.navigate("=")) {
					String cmd = position > closestPosition ? ">" : "<";
					for (int i = 0; i < absDelta; i++) {
						// TODO: do something relevant when navigate(..) returns
						// false
						c.navigate(cmd);
					}
					BigInteger ord = getCursorOrdinal(c);
					interpolator.setPoint(ord, position);
					topVisiblePosition = ord;
					return true;
				}
			}
		}
		// Exact positioning is not feasible, using interpolation
		BigInteger key = interpolator.getPoint(position);
		setCursorOrdinal(c, key);
		if (c.navigate("=>+")) {
			topVisiblePosition = getCursorOrdinal(c);
			requestRefinement(topVisiblePosition, false);
			return true;
		} else {
			// table became empty!
			c._clearBuffer(true);
			topVisiblePosition = BigInteger.ZERO;
			interpolator.resetToEmptyTable();
			return false;
		}
	}

	/**
	 * Adjusts internal state for pre-positioned cursor.
	 * 
	 * @param c
	 *            Cursor that is set to a certain position.
	 * @throws CelestaException
	 *             e. g. wrong cursor
	 */
	public void setPosition(BasicCursor c) throws CelestaException {
		checkMeta(c);
		topVisiblePosition = getCursorOrdinal(c);
		requestRefinement(topVisiblePosition, false);
	}

	private void requestRefinement(BigInteger key, boolean immediate) throws CelestaException {
		// do not process one request twice in a row
		if (key.equals(latestRequest))
			return;
		latestRequest = key;

		task = new RequestTask(key, immediate);
		if (counterThread == null || !counterThread.isAlive()) {
			counterThread = new CounterThread();
			counterThread.start();
		}
	}

	private synchronized BigInteger getCursorOrdinal(BasicCursor c) throws CelestaException {
		int i = 0;
		Object[] values = c._currentValues();
		KeyEnumerator km;
		for (String cname : closedCopy.meta().getColumns().keySet()) {
			km = keyEnumerators.get(cname);
			if (km != null)
				km.setValue(values[i]);
			i++;
		}
		return rootKeyEnumerator.getOrderValue();
	}

	private synchronized void setCursorOrdinal(BasicCursor c, BigInteger key) throws CelestaException {
		rootKeyEnumerator.setOrderValue(key);
		for (Map.Entry<String, KeyEnumerator> e : keyEnumerators.entrySet()) {
			c.setValue(e.getKey(), e.getValue().getValue());
		}
	}

	/**
	 * Returns scrollbar's knob position for current cursor value.
	 */
	public int getTopVisiblePosition() {
		return interpolator.getApproximatePosition(topVisiblePosition);
	}

	private KeyEnumerator createFieldKeyManager(ColumnMeta m) throws CelestaException {
		KeyEnumerator result;

		if (BooleanColumn.CELESTA_TYPE.equals(m.getCelestaType()))
			result = new BitFieldEnumerator();
		else if (IntegerColumn.CELESTA_TYPE.equals(m.getCelestaType()))
			result = new IntFieldEnumerator();
		else if (m instanceof StringColumn) {
			StringColumn s = (StringColumn) m;
			if (s.isMax())
				throw new CelestaException("TEXT field cannot be used as a key field in a grid.");
			result = new VarcharFieldEnumerator(s.getLength());
		} else {
			throw new CelestaException("The field with type '%s' cannot be used as a key field in a grid.",
					m.getCelestaType());
		}

		if (m.isNullable()) {
			result = NullableFieldEnumerator.create(DBAdaptor.getAdaptor().nullsFirst(), result);
		}
		return result;
	}

	private void checkMeta(BasicCursor c) throws CelestaException {
		if (c.meta() != closedCopy.meta())
			throw new CelestaException("Metaobjects for cursor and cursor position specifier don't match.");
	}

	/**
	 * Returns (approximate) total record count.
	 * 
	 * Just after creation of this object this method returns DEFAULT_COUNT
	 * value, but it asynchronously requests total count right after constructor
	 * execution.
	 */
	public int getApproxTotalCount() {
		return interpolator.getApproximateCount();
	}

	/**
	 * Sets change notifier (a method that is being called when grid metrics
	 * update is ready).
	 * 
	 * @param changeNotifier
	 *            new change modifier.
	 */
	public void setChangeNotifier(Runnable changeNotifier) {
		this.changeNotifier = changeNotifier;
	}

	/**
	 * Gets change notifier.
	 */
	public Runnable getChangeNotifier() {
		return changeNotifier;
	}

	/**
	 * If the grid is scrolled less than for given amount of records, the exact
	 * positioning in cycle will be used instead of interpolation.
	 * 
	 * @param smallScroll
	 *            new value.
	 */
	public void setMaxExactScrollValue(int smallScroll) {
		this.smallScroll = smallScroll;
	}

}

/**
 * Position refinement request parameters.
 */
final class RequestTask {
	/**
	 * The miminum time, in milliseconds, without any requests for a record
	 * position, for the latest request to be executed.
	 */
	private static final long MIN_DELAY = 500;
	private final long timeToStart;
	private final BigInteger key;

	RequestTask(BigInteger key, boolean immediate) {
		this.timeToStart = System.currentTimeMillis() + (immediate ? 0 : MIN_DELAY);
		this.key = key;
	}

	long getDelayBeforeRun() {
		return timeToStart - System.currentTimeMillis();
	}

	BigInteger getKey() {
		return key;
	}
}
