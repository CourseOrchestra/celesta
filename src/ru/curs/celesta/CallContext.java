package ru.curs.celesta;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import org.python.core.PyDictionary;

import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.DBAdaptor;
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

	private final Connection conn;
	private final Grain grain;
	private final String procName;
	private final SessionContext sesContext;
	private final int dbPid;
	private final Date startTime = new Date();

	private BasicCursor lastCursor;
	private int cursorCount;

	public CallContext(Connection conn, SessionContext sesContext) throws CelestaException {
		this.conn = conn;
		this.sesContext = sesContext;
		this.grain = null;
		this.procName = null;
		DBAdaptor db = DBAdaptor.getAdaptor();
		dbPid = db.getDBPid(conn);
	}

	public CallContext(Connection conn, SessionContext sesContext, Grain curGrain, String procName)
			throws CelestaException {
		this.conn = conn;
		this.sesContext = sesContext;
		this.grain = curGrain;
		this.procName = procName;
		DBAdaptor db = DBAdaptor.getAdaptor();
		dbPid = db.getDBPid(conn);
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
	 * Инициирует предупреждение.
	 * 
	 * @param msg
	 *            текст сообщения
	 */
	public void warning(String msg) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg));
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
		throw new CelestaException("ERROR: %s", msg);
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
}
