package ru.curs.celesta;

import java.sql.*;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Map.Entry;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyType;

import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public final class CallContext implements AutoCloseable {

	/**
	 * Максимальное число курсоров, которое может быть открыто в одном
	 * контексте.
	 */
	public static final int MAX_CURSORS = 1023;

	private static final String ERROR = "ERROR: %s";

	private static final Map<Connection, Integer> PIDSCACHE = Collections
			.synchronizedMap(new WeakHashMap<Connection, Integer>());

	private final Celesta celesta;
	private final ConnectionPool connectionPool;
	private final Connection conn;
	private final Score score;
	private final Grain grain;
	private final String procName;
	private final SessionContext sesContext;
	private final ShowcaseContext showcaseContext;
	private final DBAdaptor dbAdaptor;
	private final PermissionManager permissionManager;
	private final LoggingManager loggingManager;

	private final int dbPid;
	private final Date startTime = new Date();

	private BasicCursor lastCursor;
	private int cursorCount;

	private boolean closed = false;

	private final HashMap<PyString, PyObject> cursorsCache = new HashMap<>();

	public CallContext(CallContext context, Celesta celesta, ConnectionPool connectionPool, SessionContext sesContext,
					   ShowcaseContext showcaseContext, Score score, Grain curGrain, String procName,
					   DBAdaptor dbAdaptor, PermissionManager permissionManager, LoggingManager loggingManager)
			throws CelestaException {

		if (context != null) {
			this.connectionPool = context.connectionPool;
			this.score = context.score;
			this.permissionManager = context.permissionManager;
			this.loggingManager = context.loggingManager;
			this.celesta = context.celesta;
			this.dbAdaptor = context.dbAdaptor;
		} else {
			this.connectionPool = connectionPool;
			this.score = score;
			this.permissionManager = permissionManager;
			this.loggingManager = loggingManager;
			this.celesta = celesta;
			this.dbAdaptor = dbAdaptor;
		}

		this.conn = this.connectionPool.get();
		this.sesContext = sesContext;
		this.grain = curGrain;
		this.procName = procName;
		this.showcaseContext = showcaseContext;

		this.dbPid = PIDSCACHE.computeIfAbsent(this.conn, this.dbAdaptor::getDBPid);
	}

	/**
	 * Duplicates callcontext with another JDBC connection.
	 *
	 * @throws CelestaException
	 *             cannot create adaptor
	 */
	public CallContext getCopy() throws CelestaException {
		return new CallContextBuilder()
				.setCelesta(celesta)
				.setConnectionPool(connectionPool)
				.setSesContext(sesContext)
				.setShowcaseContext(showcaseContext)
				.setScore(score)
				.setCurGrain(grain)
				.setProcName(procName)
				.setDbAdaptor(dbAdaptor)
				.setPermissionManager(permissionManager)
				.setLoggingManager(loggingManager)
				.createCallContext();
	}

	/**
	 * Соединение с базой данных.
	 */
	public Connection getConn() {
		return conn;
	}

	/**
	 * Идентификатор пользователя.
	 */
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
	 * Данные сессии.
	 */
	public PyDictionary getData() {
		return sesContext.getData();
	}

	/**
	 * Коммитит транзакцию.
	 * 
	 * @throws CelestaException
	 *             в случае проблемы с БД.
	 */
	public void commit() throws CelestaException {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new CelestaException("Commit unsuccessful: %s", e.getMessage());
		}
	}

	/**
	 * Инициирует информационное сообщение.
	 * 
	 * @param msg
	 *            текст сообщения
	 */
	public void message(String msg) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg));
	}

	/**
	 * Инициирует информационное сообщение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * @param caption
	 *            Заголовок окна.
	 */
	public void message(String msg, String caption) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg, caption));
	}

	/**
	 * Инициирует информационное сообщение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * 
	 * @param caption
	 *            Заголовок окна.
	 * 
	 * @param subkind
	 *            Субтип сообщения.
	 * 
	 */
	public void message(String msg, String caption, String subkind) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg, caption, subkind));
	}

	/**
	 * Инициирует предупреждение.
	 * 
	 * @param msg
	 *            текст сообщения
	 */
	public void warning(String msg) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg));
	}

	/**
	 * Инициирует предупреждение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * @param caption
	 *            Заголовок окна.
	 */
	public void warning(String msg, String caption) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg, caption));
	}

	/**
	 * Инициирует предупреждение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * 
	 * @param caption
	 *            Заголовок окна.
	 * 
	 * @param subkind
	 *            Субтип сообщения.
	 * 
	 */
	public void warning(String msg, String caption, String subkind) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg, caption, subkind));
	}

	/**
	 * Инициирует ошибку и вызывает исключение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * @throws CelestaException
	 *             во всех случаях, при этом с переданным текстом
	 */
	public void error(String msg) throws CelestaException {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg));
		throw new CelestaException(ERROR, msg);
	}

	/**
	 * Инициирует ошибку и вызывает исключение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * @param caption
	 *            Заголовок окна.
	 * @throws CelestaException
	 *             во всех случаях, при этом с переданным текстом
	 */
	public void error(String msg, String caption) throws CelestaException {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg, caption));
		throw new CelestaException(ERROR, msg);
	}

	/**
	 * Инициирует ошибку и вызывает исключение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * @param caption
	 *            Заголовок окна.
	 * @param subkind
	 *            Субтип сообщения.
	 * @throws CelestaException
	 *             во всех случаях, при этом с переданным текстом
	 */
	public void error(String msg, String caption, String subkind) throws CelestaException {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg, caption, subkind));
		throw new CelestaException(ERROR, msg);
	}

	/**
	 * Возвращает экземпляр Celesta, использованный при создании контекста
	 * вызова.
	 * 
	 * @throws CelestaException
	 *             ошибка инициализации Celesta
	 */
	public Celesta getCelesta() throws CelestaException {
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

	/**
	 * Установка последнего курсора в контексте.
	 * 
	 * @param c
	 *            Курсор.
	 */
	public void setLastCursor(BasicCursor c) {
		lastCursor = c;
	}

	/**
	 * Увеличивает счетчик открытых курсоров.
	 * 
	 * @throws CelestaException
	 *             Если число открытых курсоров превысило критический порог.
	 */
	public void incCursorCount() throws CelestaException {
		if (cursorCount > MAX_CURSORS)
			throw new CelestaException("Too many cursors created in one Celesta procedure call. Check for leaks!");
		cursorCount++;
	}

	/**
	 * Уменьшает счетчик открытых курсоров.
	 */
	public void decCursorCount() {
		cursorCount--;
	}

	/**
	 * Получает последний курсор.
	 */
	public BasicCursor getLastCursor() {
		return lastCursor;
	}

	/**
	 * Закрытие всех курсоров.
	 */
	private void closeCursors() {
		while (lastCursor != null) {
			lastCursor.close();
		}
		closed = true;
	}

	/**
	 * Возвращает Process Id текущего подключения к базе данных.
	 * 
	 * @throws CelestaException
	 *             Если подключение закрылось.
	 */
	public int getDBPid() throws CelestaException {

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

	/**
	 * Был ли контекст закрыт.
	 */
	public boolean isClosed() {
		return closed;
	}

	public PermissionManager getPermissionManager() {
		return permissionManager;
	}

	public LoggingManager getLoggingManager() {
		return loggingManager;
	}

	public DBAdaptor getDbAdaptor() {
		return dbAdaptor;
	}

	/**
	 * Возвращает объект курсора cursorClass.
	 * 
	 * Все созданные курсоры кэшируются и возвращаются при последюущем запросе
	 * cursorClass. Каждый возвращаемый курсор предварительно очищается
	 * (clear()).
	 * 
	 * @param cursorClass
	 *            класс курсора
	 * @return объект курсора
	 */
	public PyObject create(final PyType cursorClass) throws CelestaException {
		PyString classId = cursorClass.__str__();
		PyObject cur = cursorsCache.computeIfAbsent(classId, (s) -> cursorClass.__call__(Py.java2py(this)));
		BasicCursor basicCur = (BasicCursor) cur.__tojava__(BasicCursor.class);
		basicCur.clear();
		return cur;
	}

	/**
	 * Удалят курсор из кэша курсоров.
	 * 
	 * Метод предназначен для внутреннего использования.
	 * 
	 * @param cursor
	 *            Объект курсора.
	 */
	public void removeFromCache(final BasicCursor cursor) {
		Iterator<Entry<PyString, PyObject>> i = cursorsCache.entrySet().iterator();
		while (i.hasNext()) {
			Entry<PyString, PyObject> e = i.next();
			BasicCursor basicCur = (BasicCursor) e.getValue().__tojava__(BasicCursor.class);
			if (cursor.equals(basicCur)) {
				i.remove();
			}
		}
	}

	@Override
	public void close() throws CelestaException {
		try {
			closeCursors();
			conn.close();
		} catch (Exception e) {
			throw new CelestaException("Can't close callContext", e);
		}
	}

	void rollback() throws SQLException {
		conn.rollback();
	}
}
