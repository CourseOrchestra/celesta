package ru.curs.celesta;

import java.sql.*;
import java.util.Date;
import java.util.HashMap;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyType;

import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.score.Grain;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public final class CallContext {

	/**
	 * Максимальное число курсоров, которое может быть открыто в одном
	 * контексте.
	 */
	public static final int MAX_CURSORS = 1023;

	private static final String ERROR = "ERROR: %s";

	private final Connection conn;
	private final Grain grain;
	private final String procName;
	private final SessionContext sesContext;
	private final ShowcaseContext showcaseContext;

	private final int dbPid;
	private final Date startTime = new Date();

	private BasicCursor lastCursor;
	private int cursorCount;

	private boolean closed = false;
	
	private final HashMap<PyString, PyObject> cursorsCache = new HashMap<>(); 

	public CallContext(Connection conn, SessionContext sesContext) throws CelestaException {
		this(conn, sesContext, null, null, null);
	}

	public CallContext(Connection conn, SessionContext sesContext, Grain curGrain, String procName)
			throws CelestaException {
		this(conn, sesContext, null, curGrain, procName);
	}

	public CallContext(Connection conn, SessionContext sesContext, ShowcaseContext showcaseContext, Grain curGrain,
			String procName) throws CelestaException {
		this.conn = conn;
		this.sesContext = sesContext;
		this.grain = curGrain;
		this.procName = procName;
		this.showcaseContext = showcaseContext;
		DBAdaptor db = DBAdaptor.getAdaptor();
		dbPid = db.getDBPid(conn);
	}

	/**
	 * Duplicates callcontext with given JDBC connection.
	 * 
	 * @param c
	 *            new connection
	 * @throws CelestaException
	 *             cannot create adaptor
	 */
	public CallContext getCopy(Connection c) throws CelestaException {
		return new CallContext(c, sesContext, showcaseContext, grain, procName);
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
		return Celesta.getInstance();
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
	public void closeCursors() {
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
	
	/**
	 * Возвращает объект курсора cursorClass.
	 * 
	 * Все созданные курсоры кэшируются и возвращаются при последюущем запросе
	 * cursorClass. 
	 * Каждый возвращаемый курсор предварительно очищается (clear()).
	 * 
	 * @param cursorClass класс курсора
	 * @return объект курсора
	 */
	public PyObject create(final PyType cursorClass) throws CelestaException {
		PyString classId = cursorClass.__str__();
		PyObject cur = cursorsCache.get(classId);
		if (cur == null) {
			cur = cursorClass.__call__(Py.java2py(this));
			cursorsCache.put(classId, cur);
		}
		
		BasicCursor basicCur = (BasicCursor)cur.__tojava__(BasicCursor.class);
		basicCur.clear();
		return cur;
	}
	
	/**
	 * Удалят курсор из кэша курсоров.
	 * 
	 * Метод предназначен для внутреннего использования.
	 * 
	 * @param cursor Объект курсора.
	 */
	public void removeFromCache(final BasicCursor cursor) {
		PyString classId = Py.java2py(cursor).getType().__str__();
		cursorsCache.remove(classId);
	}
}
