package ru.curs.celesta;

import java.sql.*;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.WeakHashMap;


import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 *
 * @param <T> heir class of CallContext
 * @param <R> heir class of SessionContext
 * @param <E> class of data accessor to create and put into cash (crutch, must be removed, cuz it's a PyType in celesta-module)
 * @param <F>  class of created data accessor (crutch, must be removed, cuz it's a PyObject in celesta-module)
 *           Types E and F are temporary parameters. CallContext must not resolve purposes of cursors caching.
 */
public abstract class CallContext
		<
		T extends CallContext<T, R, E, F>, R extends SessionContext, E extends Object, F extends Object
		>
		implements ICallContext {

	/**
	 * Максимальное число объектов доступа, которое может быть открыто в одном
	 * контексте.
	 */
	public static final int MAX_DATA_ACCESSORS = 1023;

	private static final Map<Connection, Integer> PIDSCACHE = Collections
			.synchronizedMap(new WeakHashMap<Connection, Integer>());

	protected final ICelesta celesta;
	protected final ConnectionPool connectionPool;
	private final Connection conn;
	protected final Score score;
	protected final Grain grain;
	protected final String procName;
	protected final R sesContext;
	protected final ShowcaseContext showcaseContext;
	protected final DBAdaptor dbAdaptor;
	protected final IPermissionManager permissionManager;
	protected final ILoggingManager loggingManager;

	private final int dbPid;
	private final Date startTime = new Date();

	private BasicDataAccessor lastDataAccessor;
	private int dataAccessorsCount;

	private boolean closed = false;

	public CallContext(CallContextBuilder<? extends CallContextBuilder, T, R> contextBuilder) {

		CallContext context = contextBuilder.callContext;

		if (context != null) {
			this.connectionPool = context.connectionPool;
			this.score = context.score;
			this.permissionManager = context.permissionManager;
			this.loggingManager = context.loggingManager;
			this.celesta = context.celesta;
			this.dbAdaptor = context.dbAdaptor;
		} else {
			this.connectionPool = contextBuilder.connectionPool;
			this.score = contextBuilder.score;
			this.permissionManager = contextBuilder.permissionManager;
			this.loggingManager = contextBuilder.loggingManager;
			this.celesta = contextBuilder.celesta;
			this.dbAdaptor = contextBuilder.dbAdaptor;
		}

		this.conn = this.connectionPool.get();
		this.sesContext = contextBuilder.sesContext;
		this.grain = contextBuilder.curGrain;
		this.procName = contextBuilder.procName;
		this.showcaseContext = contextBuilder.showcaseContext;

		this.dbPid = PIDSCACHE.computeIfAbsent(this.conn, this.dbAdaptor::getDBPid);
	}

	/**
	 * Duplicates callcontext with another JDBC connection.
	 */
	public abstract T getCopy();

	public Connection getConn() {
		return conn;
	}

	public String getUserId() {
		return sesContext.getUserId();
	}

	/**
	 * Идентификатор сессии.
	 */
	public String getSessionId() {
		return sesContext.getSessionId();
	}



	/**
	 * Коммитит транзакцию.
	 */
	public void commit() {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new CelestaException("Commit unsuccessful: %s", e.getMessage());
		}
	}

	public ICelesta getCelesta() {
		return celesta;
	}

	public Score getScore() {
		return score;
	}

	/**
	 * Возвращает текущую гранулу (к которой относится вызываемая функция).
	 */
	public Grain getGrain() {
		return grain;
	}

	public void setLastDataAccessor(BasicDataAccessor dataAccessor) {
		lastDataAccessor = dataAccessor;
	}

	public void incDataAccessorsCount() {
		if (dataAccessorsCount > MAX_DATA_ACCESSORS)
			throw new CelestaException("Too many data accessors created in one Celesta procedure call. Check for leaks!");
		dataAccessorsCount++;
	}

	/**
	 * Уменьшает счетчик открытых объектов доступа.
	 */
	public void decDataAccessorsCount() {
		dataAccessorsCount--;
	}

	/**
	 * Получает последний объект доступа.
	 */
	public BasicDataAccessor getLastDataAccessor() {
		return lastDataAccessor;
	}

	/**
	 * Закрытие всех классов доступа.
	 */
	private void closeDataAccessors() {
		while (lastDataAccessor != null) {
			lastDataAccessor.close();
		}
		closed = true;
	}

	/**
	 * Возвращает Process Id текущего подключения к базе данных.
	 */
	public int getDBPid() {
		return dbPid;
	}

	/**
	 * Возвращает имя процедуры, которая была изначально вызвана.
	 */
	public String getProcName() {
		return procName;
	}

	/**
	 * Возвращает время создания контекста вызова.
	 */
	public Date getStartTime() {
		return startTime;
	}

	/**
	 * Возвращает контексты Showcase.
	 */
	public ShowcaseContext getShowcaseContext() {
		return showcaseContext;
	}

	public boolean isClosed() {
		return closed;
	}

	public IPermissionManager getPermissionManager() {
		return permissionManager;
	}

	public ILoggingManager getLoggingManager() {
		return loggingManager;
	}

	public DBAdaptor getDbAdaptor() {
		return dbAdaptor;
	}

	@Override
	public void close() {
		try {
			closeDataAccessors();
			conn.close();
		} catch (Exception e) {
			throw new CelestaException("Can't close callContext", e);
		}
	}

	void rollback() throws SQLException {
		conn.rollback();
	}


	/**
	 * Возвращает объект доступа dataAccessorClass.
	 *
	 * Все созданные объекты доступа кэшируются и возвращаются при последюущем запросе
	 * dataAccessorClass. Каждый возвращаемый объект доступа предварительно очищается
	 * (clear()).
	 *
	 * @param dataAccessorClass
	 *            класс объекта доступа
	 * @return объект доступа
	 */
	public abstract F create(E dataAccessorClass);

	public abstract void removeFromCache(BasicDataAccessor dataAccessor);

	public static abstract class CallContextBuilder<T extends CallContextBuilder<T, R, E>,
			R extends CallContext, E extends SessionContext> {

		protected R callContext = null;
		protected ICelesta celesta = null;
		protected ConnectionPool connectionPool = null;
		protected E sesContext = null;
		protected Score score = null;
		protected ShowcaseContext showcaseContext = null;
		protected Grain curGrain = null;
		protected String procName = null;
		protected DBAdaptor dbAdaptor = null;
		protected IPermissionManager permissionManager;
		protected ILoggingManager loggingManager;


		protected abstract T getThis();

		public T setCallContext(R callContext) {
			this.callContext = callContext;
			return getThis();
		}

		public T setCelesta(ICelesta celesta) {
			this.celesta = celesta;
			return getThis();
		}

		public T setConnectionPool(ConnectionPool connectionPool) {
			this.connectionPool = connectionPool;
			return getThis();
		}

		public T setSesContext(E sesContext) {
			this.sesContext = sesContext;
			return getThis();
		}

		public T setScore(Score score) {
			this.score = score;
			return getThis();
		}

		public T setShowcaseContext(ShowcaseContext showcaseContext) {
			this.showcaseContext = showcaseContext;
			return getThis();
		}

		public T setCurGrain(Grain curGrain) {
			this.curGrain = curGrain;
			return getThis();
		}

		public T setProcName(String procName) {
			this.procName = procName;
			return getThis();
		}

		public T setDbAdaptor(DBAdaptor dbAdaptor) {
			this.dbAdaptor = dbAdaptor;
			return getThis();
		}

		public T setPermissionManager(IPermissionManager permissionManager) {
			this.permissionManager = permissionManager;
			return getThis();
		}

		public T setLoggingManager(ILoggingManager loggingManager) {
			this.loggingManager = loggingManager;
			return getThis();
		}

		public abstract R createCallContext();
	}
}
