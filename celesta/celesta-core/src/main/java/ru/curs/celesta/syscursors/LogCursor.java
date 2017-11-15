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
 * Курсор на таблице log.
 */
public final class LogCursor extends SysCursor {

	public static final String TABLE_NAME = "log";

	private Integer entryno;
	private String userid;
	private String sessionid;
	private String grainid;
	private String tablename;
	private String pkvalue1;
	private String pkvalue2;
	private String pkvalue3;
	private String oldvalues;
	private String newvalues;
	// CHECKSTYLE:OFF
	// Поля базы данных
	private Date entry_time;
	private String action_type;

	// CHECKSTYLE:ON

	public LogCursor(CallContext context) throws CelestaException {
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
		entry_time = rs.getTimestamp("entry_time");
		userid = rs.getString("userid");
		grainid = rs.getString("grainid");
		tablename = rs.getString("tablename");
		action_type = rs.getString("action_type");
		pkvalue1 = rs.getString("pkvalue1");
		pkvalue2 = rs.getString("pkvalue2");
		pkvalue3 = rs.getString("pkvalue3");
		oldvalues = rs.getString("oldvalues");
		newvalues = rs.getString("newvalues");
		sessionid = rs.getString("sessionid");
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys)
			entryno = null;
		entry_time = null;
		userid = null;
		grainid = null;
		tablename = null;
		action_type = null;
		pkvalue1 = null;
		pkvalue2 = null;
		pkvalue3 = null;
		oldvalues = null;
		newvalues = null;
		sessionid = null;
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
		Object[] result = { entryno, entry_time, userid, sessionid, grainid,
				tablename, action_type, pkvalue1, pkvalue2, pkvalue3,
				oldvalues, newvalues };
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

	// CHECKSTYLE:OFF
	// так (с подчёркиванием) называются поля в базе данных
	/**
	 * Время записи.
	 */
	public Date getEntry_time() {
		return entry_time;
	}

	/**
	 * Устанавливает время записи.
	 * 
	 * @param entryTime
	 *            the entryTime to set
	 */
	public void setEntry_time(Date entryTime) {
		this.entry_time = entryTime;
	}

	/**
	 * Тип действия (I, M, D).
	 */
	public String getAction_type() {
		return action_type;
	}

	/**
	 * Устанавливает тип действия.
	 * 
	 * @param actionType
	 *            the actionType to set
	 */
	public void setAction_type(String actionType) {
		this.action_type = actionType;
	}

	// CHECKSTYLE:ON

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
	 * Идентификатор гранулы.
	 */
	public String getGrainid() {
		return grainid;
	}

	/**
	 * Устанавливает идентификатор гранулы.
	 * 
	 * @param grainid
	 *            the grainid to set
	 */
	public void setGrainid(String grainid) {
		this.grainid = grainid;
	}

	/**
	 * Имя таблицы.
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * Устанавливает имя таблицы.
	 * 
	 * @param tablename
	 *            the tablename to set
	 */
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	/**
	 * Значение первого поля первичного ключа.
	 */
	public String getPkvalue1() {
		return pkvalue1;
	}

	/**
	 * Устанавливает значение первого поля первичного ключа.
	 * 
	 * @param pkvalue1
	 *            the pkvalue1 to set
	 */
	public void setPkvalue1(String pkvalue1) {
		this.pkvalue1 = pkvalue1;
	}

	/**
	 * Значение второго поля первичного ключа.
	 */
	public String getPkvalue2() {
		return pkvalue2;
	}

	/**
	 * Устанавливает значение второго поля первичного ключа.
	 * 
	 * @param pkvalue2
	 *            the pkvalue2 to set
	 */
	public void setPkvalue2(String pkvalue2) {
		this.pkvalue2 = pkvalue2;
	}

	/**
	 * Значение третьего поля первичного ключа.
	 */
	public String getPkvalue3() {
		return pkvalue3;
	}

	/**
	 * Устанавливает значение третьего поля первичного ключа.
	 * 
	 * @param pkvalue3
	 *            the pkvalue3 to set
	 */
	public void setPkvalue3(String pkvalue3) {
		this.pkvalue3 = pkvalue3;
	}

	/**
	 * Прежнее состояние записи.
	 * 
	 * @return the oldvalues
	 */
	public String getOldvalues() {
		return oldvalues;
	}

	/**
	 * Устанавливает прежнее состояние записи.
	 * 
	 * @param oldvalues
	 *            the oldvalues to set
	 */
	public void setOldvalues(String oldvalues) {
		this.oldvalues = oldvalues;
	}

	/**
	 * Текущее состояние записи.
	 * 
	 * @return the newvalues
	 */
	public String getNewvalues() {
		return newvalues;
	}

	/**
	 * Устанавливает текущее состояние записи.
	 * 
	 * @param newvalues
	 *            the newvalues to set
	 */
	public void setNewvalues(String newvalues) {
		this.newvalues = newvalues;
	}

	@Override
	public void copyFieldsFrom(BasicCursor c) {
		LogCursor from = (LogCursor) c;
		entryno = from.entryno;
		entry_time = from.entry_time;
		userid = from.userid;
		grainid = from.grainid;
		tablename = from.tablename;
		action_type = from.action_type;
		pkvalue1 = from.pkvalue1;
		pkvalue2 = from.pkvalue2;
		pkvalue3 = from.pkvalue3;
		oldvalues = from.oldvalues;
		newvalues = from.newvalues;
		sessionid = from.sessionid;
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
		// CHECKSTYLE:ON
		LogCursor result = new LogCursor(context);
		result.copyFieldsFrom(this);
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _setAutoIncrement(int val) {
		// CHECKSTYLE:ON
		entryno = val;
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
