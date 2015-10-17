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
import ru.curs.lyra.grid.BitFieldMgr;
import ru.curs.lyra.grid.CompositeKeyManager;
import ru.curs.lyra.grid.IntFieldMgr;
import ru.curs.lyra.grid.KeyInterpolator;
import ru.curs.lyra.grid.KeyManager;
import ru.curs.lyra.grid.VarcharFieldMgr;

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
	private final KeyManager keyManager;
	private final Map<String, KeyManager> keyManagers = new HashMap<>();

	private final GrainElement meta;
	private final Map<String, AbstractFilter> filters;
	private final Expr cfilter;
	private final String[] names;
	private final GridRefinementCallback callback;

	private CounterThread req = null;

	private BigInteger latestRequest;

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
					for (int i = 0; i < names.length; i++) {
						Object param = valsMap.get(names[i]);
						DBAdaptor.setParam(stmt, j++, param);
					}
					try {
						// System.out.println(stmt.toString());
						ResultSet rs = stmt.executeQuery();
						rs.next();
						int result = rs.getInt(1);

						interpolator.setPoint(myRequest.getKey(), result);
						if (callback != null)
							callback.execute(myRequest.getKey(), result);
					} finally {
						stmt.close();
					}
				}
			} catch (SQLException | CelestaException | InterruptedException e) {
				// terminate thread silently
				e.printStackTrace();
				return;
			} finally {
				req = null;
				ConnectionPool.putBack(conn);
			}
		}

	}

	public GridDriver(BasicCursor c, GridRefinementCallback callback) throws CelestaException {

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
			// TODO: find minimum and maximum values for the only key field
			ColumnMeta m = meta.getColumns().get(names[0]);
			keyManager = getFieldKeyManager(m);
			keyManagers.put(names[0], keyManager);
		} else {
			// TODO: find minimum and maximum values for the first key field
			KeyManager[] km = new KeyManager[names.length];
			for (int i = 0; i < names.length; i++) {
				ColumnMeta m = meta.getColumns().get(names[i]);
				km[i] = getFieldKeyManager(m);
				keyManagers.put(names[i], km[i]);
			}
			keyManager = new CompositeKeyManager(km);
		}

		interpolator = new KeyInterpolator(BigInteger.ZERO, keyManager.cardinality().subtract(BigInteger.ONE),
				DEFAULT_COUNT);

		filters = c.getFilters();
		cfilter = c.getComplexFilterExpr();

		// Request a total record count immediately
		c.navigate("+");
		requestRefinement(c, true);
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
	 * @throws CelestaException
	 *             e.g. wrong cursor
	 */
	public void setPosition(int position, int delta, BasicCursor c) throws CelestaException {
		checkMeta(c);

		int absDelta = Math.abs(delta);
		if (absDelta < 11) {
			// Trying to perform exact positioning!
			BigInteger key = interpolator.getExactPoint(position - delta);
			if (key != null) {
				setCursorOrdValue(c, key);
				if (c.navigate("=")) {
					for (int i = 0; i < absDelta; i++) {
						c.navigate(delta > 0 ? ">" : "<");
					}
					BigInteger ord = getCursorOrdinal(c);
					interpolator.setPoint(ord, position);
					return;
				}
			}
		}
		// too big delta or exact key not found
		BigInteger key = interpolator.getPoint(position);
		setCursorOrdValue(c, key);
		String before = c._currentValues()[2].toString();
		if (delta >= 0) {
			c.navigate("=>+");
			System.out.println("=>+");
		} else {
			c.navigate("=<-");
			System.out.println("=<-");
		}
		String after = c._currentValues()[2].toString();
		System.out.printf("%s->%s%n", before, after);
		requestRefinement(c, false);
	}

	private void setCursorOrdValue(BasicCursor c, BigInteger key) throws CelestaException {
		keyManager.setOrderValue(key);
		for (Map.Entry<String, KeyManager> e : keyManagers.entrySet()) {
			c.setValue(e.getKey(), e.getValue().getValue());
		}
	}

	private void requestRefinement(BasicCursor c, boolean immediate) throws CelestaException {
		BigInteger key = getCursorOrdinal(c);
		// do not process one request twice in a row
		if (key.equals(latestRequest))
			return;
		latestRequest = key;
		RequestTask task = new RequestTask(c.getNavigationWhereClause('<'), c._currentValues(), key, immediate);
		if (req == null) {
			req = new CounterThread(task);
			req.start();
		} else {
			req.task = task;
		}
	}

	private BigInteger getCursorOrdinal(BasicCursor c) {
		int i = 0;
		Object[] values = c._currentValues();
		KeyManager km;
		for (String cname : meta.getColumns().keySet()) {
			km = keyManagers.get(cname);
			if (km != null)
				km.setValue(values[i]);
			i++;
		}
		return keyManager.getOrderValue();
	}

	/**
	 * Gets (adjusts) knob position based on cursor values.
	 * 
	 * @param c
	 *            Cursor
	 * @throws CelestaException
	 *             e.g. wrong cursor
	 */
	public int getPosition(BasicCursor c) throws CelestaException {
		checkMeta(c);
		BigInteger ordinal = getCursorOrdinal(c);
		return interpolator.getApproximatePosition(ordinal);
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

	private KeyManager getFieldKeyManager(ColumnMeta m) throws CelestaException {
		if (BooleanColumn.CELESTA_TYPE.equals(m.getCelestaType()))
			return new BitFieldMgr();
		else if (IntegerColumn.CELESTA_TYPE.equals(m.getCelestaType()))
			return new IntFieldMgr();
		else if (m instanceof StringColumn) {
			StringColumn s = (StringColumn) m;
			if (s.isMax())
				throw new CelestaException("TEXT field cannot be used as a key field in a grid.");
			return new VarcharFieldMgr(s.getLength());
		}
		throw new CelestaException("The field with type '%s' cannot be used as a key field in a grid.",
				m.getCelestaType());
	}

	private void checkMeta(BasicCursor c) throws CelestaException {
		if (c.meta() != meta)
			throw new CelestaException("Metaobjects for cursor and cursor position specifier don't match.");
	}

	// TODO: REMOVE THIS DEBUG METHOD
	public KeyInterpolator getInterpolator() {
		return interpolator;
	}

	public VarcharFieldMgr getKeyManager() {
		return (VarcharFieldMgr) keyManager;
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
