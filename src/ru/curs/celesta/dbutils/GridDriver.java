package ru.curs.celesta.dbutils;

import java.math.BigInteger;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;

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
import ru.curs.lyra.grid.VarcharFieldEnumerator;

/**
 * Specifies the record position asynchronously, using separate execution
 * thread.
 */
public final class GridDriver {

	private static final int MAX_REFINEMENTS_COUNT = 100;

	/**
	 * The default assumption for a records count in a table.
	 */
	private static final int DEFAULT_COUNT = 1024;

	private final KeyInterpolator interpolator;
	private final KeyEnumerator rootKeyEnumerator;
	private final Map<String, KeyEnumerator> keyEnumerators = new HashMap<>();
	private final Random rnd = new Random();

	private final GrainElement meta;

	/**
	 * Key columns' names.
	 */
	private final String[] names;

	private final Runnable callback;

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

	private final BasicCursor closedCopy;

	private int refinementsCount = 0;

	/**
	 * Асинхронный запрос к базе данных на уточнение позиции.
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
							if (callback != null)
								callback.run();
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
					if (callback != null)
						callback.run();
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

		this.callback = callback;

		// Getting key column names ('a key column' here is a column included
		// into
		// ORDER BY clause)
		names = c.getOrderBy().split(",");
		for (int i = 0; i < names.length; i++) {
			Matcher m = BasicCursor.QUOTED_COLUMN_NAME.matcher(names[i]);
			m.find();
			String quotedName = m.group(1);
			names[i] = quotedName.substring(1, quotedName.length() - 1);
		}

		meta = c.meta();
		// KeyManager factory
		if (names.length == 1) {
			// Single field key manager
			ColumnMeta m = meta.getColumns().get(names[0]);
			rootKeyEnumerator = createFieldKeyManager(m);
			keyEnumerators.put(names[0], rootKeyEnumerator);
		} else {
			// Multiple field key manager
			KeyEnumerator[] km = new KeyEnumerator[names.length];
			for (int i = 0; i < names.length; i++) {
				ColumnMeta m = meta.getColumns().get(names[i]);
				km[i] = createFieldKeyManager(m);
				keyEnumerators.put(names[i], km[i]);
			}
			rootKeyEnumerator = new CompositeKeyEnumerator(km);
		}

		c.navigate("+");
		BigInteger higherOrd = getCursorOrdinal(c);
		// Request a total record count immediately
		requestRefinement(higherOrd, true);

		c.navigate("-");
		BigInteger lowerOrd = getCursorOrdinal(c);
		interpolator = new KeyInterpolator(lowerOrd, higherOrd, DEFAULT_COUNT);

		topVisiblePosition = lowerOrd;

		closedCopy = c._getBufferCopy(c.callContext());
		closedCopy.copyFiltersFrom(c);
		closedCopy.copyOrderFrom(c);
		closedCopy.close();
	}

	/**
	 * Fills key fields of a cursor based on scroller knob position.
	 * 
	 * @param position
	 *            scrollbar knob position
	 * @param delta
	 *            difference from previous position
	 * @param c
	 *            Alive cursor to be modified
	 * @return The exact primary key information after positioning
	 * @throws CelestaException
	 *             e.g. wrong cursor
	 */
	public BigInteger setPosition(int position, int delta, BasicCursor c) throws CelestaException {
		checkMeta(c);

		int absDelta = Math.abs(delta);
		if (absDelta < 11) {
			// Trying to perform exact positioning!
			BigInteger key = interpolator.getExactPoint(position - delta);
			if (key != null) {
				setCursorOrdinal(c, key);
				if (c.navigate("=")) {
					for (int i = 0; i < absDelta; i++) {
						c.navigate(delta > 0 ? ">" : "<");
					}
					BigInteger ord = getCursorOrdinal(c);
					interpolator.setPoint(ord, position);
					topVisiblePosition = ord;
					return topVisiblePosition;
				}
			}
		}
		// too big delta or exact key not found
		BigInteger key = interpolator.getPoint(position);
		setCursorOrdinal(c, key);
		c.navigate(delta >= 0 ? "=>+" : "=<-");
		topVisiblePosition = getCursorOrdinal(c);
		requestRefinement(topVisiblePosition, false);
		return topVisiblePosition;
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
		for (String cname : meta.getColumns().keySet()) {
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
	 * Gets (adjusts) knob position for current cursor value.
	 * 
	 */
	public int getTopVisiblePosition() {
		return interpolator.getApproximatePosition(topVisiblePosition);
	}

	private KeyEnumerator createFieldKeyManager(ColumnMeta m) throws CelestaException {
		if (BooleanColumn.CELESTA_TYPE.equals(m.getCelestaType()))
			return new BitFieldEnumerator();
		else if (IntegerColumn.CELESTA_TYPE.equals(m.getCelestaType()))
			return new IntFieldEnumerator();
		else if (m instanceof StringColumn) {
			StringColumn s = (StringColumn) m;
			if (s.isMax())
				throw new CelestaException("TEXT field cannot be used as a key field in a grid.");
			return new VarcharFieldEnumerator(s.getLength());
		}
		throw new CelestaException("The field with type '%s' cannot be used as a key field in a grid.",
				m.getCelestaType());
	}

	private void checkMeta(BasicCursor c) throws CelestaException {
		if (c.meta() != meta)
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
	 * Returns this driver's key interpolator.
	 */
	public KeyInterpolator getInterpolator() {
		return interpolator;
	}

	/**
	 * Returns this driver's key manager.
	 */
	public KeyEnumerator getKeyEnumerator() {
		return rootKeyEnumerator;
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
