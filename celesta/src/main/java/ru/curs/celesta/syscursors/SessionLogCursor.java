package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.python.core.PyFunction;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.event.TriggerType;

/**
 * Курсор таблицы логирования входов и выходов из системы.
 */
public class SessionLogCursor extends SysCursor {

	public static final String TABLE_NAME = "sessionlog";

	private Integer entryno;
	private String sessionid;
	private String userid;
	private Date logintime;
	private Date logoutime;
	private Boolean timeout;
	private Boolean failedlogin;

	public SessionLogCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	public void copyFieldsFrom(BasicCursor c) {
		SessionLogCursor from = (SessionLogCursor) c;
		entryno = from.entryno;
		sessionid = from.sessionid;
		userid = from.userid;
		logintime = from.logintime;
		logoutime = from.logoutime;
		timeout = from.timeout;
		failedlogin = from.failedlogin;
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context) throws CelestaException {
		// CHECKSTYLE:ON
		SessionLogCursor result = new SessionLogCursor(context);
		result.copyFieldsFrom(this);
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentKeyValues() {
		// CHECKSTYLE:ON
		Object[] result = { entryno };
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys)
			entryno = null;
		sessionid = null;
		userid = null;
		logintime = null;
		logoutime = null;
		timeout = null;
		failedlogin = null;
	}

	@Override
	// CHECKSTYLE:OFF
	protected String _tableName() {
		// CHECKSTYLE:ON
		return TABLE_NAME;
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _parseResult(ResultSet rs) throws SQLException {
		// CHECKSTYLE:ON
		entryno = rs.getInt("entryno");
		sessionid = rs.getString("sessionid");
		userid = rs.getString("userid");
		logintime = rs.getTimestamp("logintime");
		logoutime = rs.getTimestamp("logoutime");
		timeout = rs.getBoolean("timeout");
		failedlogin = rs.getBoolean("failedlogin");
	}

	@Override
	// CHECKSTYLE:OFF
	public Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { entryno, sessionid, userid, logintime, logoutime,
				timeout, failedlogin };
		return result;
	}

	/**
	 * Номер записи.
	 */
	public int getEntryno() {
		return entryno;
	}

	/**
	 * Номер записи.
	 * 
	 * @param entryno
	 *            Номер записи
	 */
	public void setEntryno(int entryno) {
		this.entryno = entryno;
	}

	/**
	 * Код сессии.
	 */
	public String getSessionid() {
		return sessionid;
	}

	/**
	 * Код сессии.
	 * 
	 * @param sessionid
	 *            Код сессии
	 */
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}

	/**
	 * Код пользователя.
	 */
	public String getUserid() {
		return userid;
	}

	/**
	 * Код пользователя.
	 * 
	 * @param userid
	 *            Код пользователя.
	 */
	public void setUserid(String userid) {
		this.userid = userid;
	}

	/**
	 * Время логина.
	 */
	public Date getLogintime() {
		return logintime;
	}

	/**
	 * Время логина.
	 * 
	 * @param logintime
	 *            Время логина.
	 */
	public void setLogintime(Date logintime) {
		this.logintime = logintime;
	}

	/**
	 * Выход по таймауту.
	 */
	public boolean isTimeout() {
		return timeout;
	}

	/**
	 * Выход по таймауту.
	 * 
	 * @param timeout
	 *            выход по таймауту.
	 */
	public void setTimeout(boolean timeout) {
		this.timeout = timeout;
	}

	/**
	 * Время выхода.
	 */
	public Date getLogoutime() {
		return logoutime;
	}

	/**
	 * Время выхода.
	 * 
	 * @param logoutime
	 *            Время выхода
	 */
	public void setLogoutime(Date logoutime) {
		this.logoutime = logoutime;
	}

	/**
	 * Указывает на неудачный логин.
	 */
	public Boolean getFailedlogin() {
		return failedlogin;
	}

	/**
	 * Неудачный логин.
	 * 
	 * @param failedlogin
	 *            неудачный логин.
	 */
	public void setFailedlogin(boolean failedlogin) {
		this.failedlogin = failedlogin;
	}


	public static void onPreDelete(PyFunction pyFunction) {
		try {
			Celesta.getInstance().getTriggerDispatcher()
					.registerTrigger(TriggerType.PRE_DELETE, TABLE_NAME, pyFunction);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	public static void onPostDelete(PyFunction pyFunction) {
		try {
			Celesta.getInstance().getTriggerDispatcher()
					.registerTrigger(TriggerType.POST_DELETE, TABLE_NAME, pyFunction);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	public static void onPreUpdate(PyFunction pyFunction) {
		try {
			Celesta.getInstance().getTriggerDispatcher()
					.registerTrigger(TriggerType.PRE_UPDATE, TABLE_NAME, pyFunction);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	public static void onPostUpdate(PyFunction pyFunction) {
		try {
			Celesta.getInstance().getTriggerDispatcher()
					.registerTrigger(TriggerType.POST_UPDATE, TABLE_NAME, pyFunction);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	public static void onPreInsert(PyFunction pyFunction) {
		try {
			Celesta.getInstance().getTriggerDispatcher()
					.registerTrigger(TriggerType.PRE_INSERT, TABLE_NAME, pyFunction);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	public static void onPostInsert(PyFunction pyFunction) {
		try {
			Celesta.getInstance().getTriggerDispatcher()
					.registerTrigger(TriggerType.POST_INSERT, TABLE_NAME, pyFunction);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}
}
