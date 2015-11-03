package ru.curs.celesta.dbutils;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;
import ru.curs.lyra.grid.BitFieldEnumerator;
import ru.curs.lyra.grid.CompositeKeyEnumerator;
import ru.curs.lyra.grid.IntFieldEnumerator;
import ru.curs.lyra.grid.KeyInterpolator;
import ru.curs.lyra.grid.KeyEnumerator;
import ru.curs.lyra.grid.VarcharFieldEnumerator;

/**
 * Specifies the record position asynchronously, using separate execution
 * thread.
 */
public final class GridDriver {

	/**
	 * The default assumption for a records count in a table.
	 */
	private static final int DEFAULT_COUNT = 1024;

	private final KeyInterpolator interpolator;
	private final KeyEnumerator rootKeyEnumerator;
	private final Map<String, KeyEnumerator> keyEnumerators = new HashMap<>();

	private final GrainElement meta;
	private final Map<String, AbstractFilter> filters;
	private final Expr cfilter;
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

	/**
	 * Асинхронный запрос к базе данных на уточнение позиции.
	 */
	private final class CounterThread extends Thread {
		private RequestTask task;

		public CounterThread(RequestTask request) {
			super();
			this.task = request;
		}

		@Override
		public void run() {
			RequestTask myRequest = null;
			long delta;
			Connection conn = null;
			try {
				conn = ConnectionPool.get();
				DBAdaptor dba = DBAdaptor.getAdaptor();
				while (!(task == null || task.equals(myRequest))) {
					while ((delta = task.getDelayBeforeRun()) > 0) {
						Thread.sleep(delta);
					}
					myRequest = task;
					PreparedStatement stmt = dba.getSetCountStatement(conn, meta, filters, cfilter,
							myRequest.getWhereClause());

					// getting navigation clause params
					Map<String, Object> valsMap = new HashMap<>();
					int j = 0;
					Object[] vals = myRequest.getValues();
					for (String colName : meta.getColumns().keySet())
						valsMap.put(colName, vals[j++]);

					j = dba.fillSetQueryParameters(filters, stmt);
					// System.out.println(stmt.toString());
					for (int i = 0; i < names.length; i++) {
						Object param = valsMap.get(names[i]);
						DBAdaptor.setParam(stmt, j++, param);
						if (i < names.length - 1) {
							DBAdaptor.setParam(stmt, j++, param);
						}
					}
					try {
						// System.out.println(stmt.toString());
						ResultSet rs = stmt.executeQuery();
						rs.next();
						int result = rs.getInt(1);

						interpolator.setPoint(myRequest.getKey(), result);
						if (callback != null)
							callback.run();
					} finally {
						stmt.close();
					}
				}
			} catch (SQLException | CelestaException | InterruptedException e) {
				// terminate thread silently
				e.printStackTrace();
				return;
			} finally {
				counterThread = null;
				ConnectionPool.putBack(conn);
			}
		}

	}

	public GridDriver(BasicCursor c, Runnable callback) throws CelestaException {

		this.callback = callback;

		// Getting key column names (key column here is a column included into
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
		requestRefinement(c, true);

		c.navigate("-");
		BigInteger lowerOrd = getCursorOrdinal(c);
		interpolator = new KeyInterpolator(lowerOrd, higherOrd, DEFAULT_COUNT);

		filters = c.getFilters();
		cfilter = c.getComplexFilterExpr();
		topVisiblePosition = lowerOrd;
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
		requestRefinement(c, false);
		topVisiblePosition = getCursorOrdinal(c);
		return topVisiblePosition;
	}

	private void requestRefinement(BasicCursor c, boolean immediate) throws CelestaException {
		BigInteger key = getCursorOrdinal(c);
		// do not process one request twice in a row
		if (key.equals(latestRequest))
			return;
		latestRequest = key;

		RequestTask task = new RequestTask(c.getNavigationWhereClause('<'), c._currentValues(), key, immediate);
		if (counterThread == null) {
			counterThread = new CounterThread(task);
			counterThread.start();
		} else {
			counterThread.task = task;
		}
	}

	private BigInteger getCursorOrdinal(BasicCursor c) throws CelestaException {
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

	private void setCursorOrdinal(BasicCursor c, BigInteger key) throws CelestaException {
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
 * Параметры запроса на уточнение позиции.
 */
final class RequestTask {
	/**
	 * The miminum time, in milliseconds, without any requests for a record
	 * position, for the latest request to be executed.
	 */
	private static final long MIN_DELAY = 500;

	private final String whereClause;
	private final Object[] values;
	private final long timeToStart;
	private final BigInteger key;

	RequestTask(String whereClause, Object[] values, BigInteger key, boolean immediate) {
		this.whereClause = whereClause;
		this.values = values;
		this.timeToStart = System.currentTimeMillis() + (immediate ? 0 : MIN_DELAY);
		this.key = key;
	}

	long getDelayBeforeRun() {
		return timeToStart - System.currentTimeMillis();
	}

	@Override
	public boolean equals(Object obj) {
		return super.equals(obj) || (obj instanceof RequestTask && key.equals(((RequestTask) obj).key));
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	String getWhereClause() {
		return whereClause;
	}

	BigInteger getKey() {
		return key;
	}

	Object[] getValues() {
		return values;
	}

}
