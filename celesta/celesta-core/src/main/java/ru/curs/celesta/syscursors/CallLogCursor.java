package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.python.core.PyFunction;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.event.TriggerType;

/**
 * Курсор на таблице Calllog.
 */
public final class CallLogCursor extends SysCursor {

  public static final String TABLE_NAME = "calllog";

	private Integer entryno;
	private String userid;
	private String sessionid;
	private String procname;
	private Date starttime;
	private Integer duration;

	// CHECKSTYLE:ON

	public CallLogCursor(CallContext context) throws CelestaException {
		super(context);
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
		userid = rs.getString("userid");
		sessionid = rs.getString("sessionid");
		procname = rs.getString("procname");
		starttime = rs.getDate("starttime");
		duration = rs.getInt("duration");
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys)
			entryno = null;
		userid = null;
		sessionid = null;
		procname = null;
		starttime = null;
		duration = null;
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
	public Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { entryno, sessionid, userid, procname, starttime, duration };
		return result;
	}

	/**
	 * Номер записи.
	 */
	public Integer getEntryno() {
		return entryno;
	}

	/**
	 * Устанавливает номер записи.
	 * 
	 * @param entryno
	 *            the entryno to set
	 */
	public void setEntryno(int entryno) {
		this.entryno = entryno;
	}

	/**
	 * Время записи.
	 */
	public Date getStarttime() {
		return starttime;
	}

	/**
	 * Устанавливает время записи.
	 * 
	 * @param starttime
	 *            the entryTime to set
	 */
	public void setStarttime(Date starttime) {
		this.starttime = starttime;
	}

	/**
	 * Идентификатор пользователя, произведшего изменение.
	 */
	public String getUserid() {
		return userid;
	}

	/**
	 * Устанавливает идентификатор пользователя.
	 * 
	 * @param userid
	 *            the userid to set
	 */
	public void setUserid(String userid) {
		this.userid = userid;
	}

	/**
	 * Возвращает id сессии.
	 */
	public String getSessionid() {
		return sessionid;
	}

	/**
	 * Устанавливает id сессии.
	 * 
	 * @param sessionid
	 *            новое значение.
	 */
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}

	/**
	 * Возвращает имя вызванной процедуры.
	 */
	public String getProcname() {
		return procname;
	}

	/**
	 * Устанавливает имя вызванной процедуры.
	 * 
	 * @param procname
	 *            имя вызванной процедуры.
	 */
	public void setProcname(String procname) {
		this.procname = procname;
	}

	/**
	 * Возвращает продолжительность работы процедуры.
	 */
	public Integer getDuration() {
		return duration;
	}

	/**
	 * Устанавливает продолжительность работы процедуры.
	 * 
	 * @param duration
	 *            имя вызванной процедуры.
	 */
	public void setDuration(int duration) {
		this.duration = duration;
	}

	@Override
	public void copyFieldsFrom(BasicCursor c) {
		CallLogCursor from = (CallLogCursor) c;
		entryno = from.entryno;
		starttime = from.starttime;
		userid = from.userid;
		sessionid = from.sessionid;
		procname = from.procname;
		duration = from.duration;
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
		// CHECKSTYLE:ON
		CallLogCursor result = new CallLogCursor(context);
		result.copyFieldsFrom(this);
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _setAutoIncrement(int val) {
		// CHECKSTYLE:ON
		entryno = val;
	}


	public static void onPreDelete(PyFunction pyFunction) {
		Celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_DELETE, TABLE_NAME, pyFunction);
	}

	public static void onPostDelete(PyFunction pyFunction) {
		Celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_DELETE, TABLE_NAME, pyFunction);
	}

	public static void onPreUpdate(PyFunction pyFunction) {
		Celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_UPDATE, TABLE_NAME, pyFunction);
	}

	public static void onPostUpdate(PyFunction pyFunction) {
		Celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_UPDATE, TABLE_NAME, pyFunction);
	}

	public static void onPreInsert(PyFunction pyFunction) {
		Celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_INSERT, TABLE_NAME, pyFunction);
	}

	public static void onPostInsert(PyFunction pyFunction) {
		Celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_INSERT, TABLE_NAME, pyFunction);
	}
}
