package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице log.
 */
public final class LogCursor extends SysCursor {

	private Integer entryno;
	private Date entryTime;
	private String userid;
	private String grainid;
	private String tablename;
	private String actionType;
	private String pkvalue1;
	private String pkvalue2;
	private String pkvalue3;
	private String oldvalues;
	private String newvalues;

	public LogCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	// CHECKSTYLE:OFF
	protected String _tableName() {
		// CHECKSTYLE:ON
		return "log";
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _parseResult(ResultSet rs) throws SQLException {
		// CHECKSTYLE:ON
		entryno = rs.getInt("entryno");
		entryTime = rs.getDate("entry_time");
		userid = rs.getString("userid");
		grainid = rs.getString("grainid");
		tablename = rs.getString("tablename");
		actionType = rs.getString("action_type");
		pkvalue1 = rs.getString("pkvalue1");
		pkvalue2 = rs.getString("pkvalue2");
		pkvalue3 = rs.getString("pkvalue3");
		oldvalues = rs.getString("oldvalues");
		newvalues = rs.getString("newvalues");
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys)
			entryno = null;
		entryTime = null;
		userid = null;
		grainid = null;
		tablename = null;
		actionType = null;
		pkvalue1 = null;
		pkvalue2 = null;
		pkvalue3 = null;
		oldvalues = null;
		newvalues = null;
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
	protected Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { entryno, entryTime, userid, grainid, tablename,
				actionType, pkvalue1, pkvalue2, pkvalue3, oldvalues, newvalues };
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
	public Date getEntryTime() {
		return entryTime;
	}

	/**
	 * Устанавливает время записи.
	 * 
	 * @param entryTime
	 *            the entryTime to set
	 */
	public void setEntryTime(Date entryTime) {
		this.entryTime = entryTime;
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
	 * Тип действия (I, M, D).
	 */
	public String getActionType() {
		return actionType;
	}

	/**
	 * Устанавливает тип действия.
	 * 
	 * @param actionType
	 *            the actionType to set
	 */
	public void setActionType(String actionType) {
		this.actionType = actionType;
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
	public void copyFieldsFrom(Cursor c) {
		LogCursor from = (LogCursor) c;
		entryno = from.entryno;
		entryTime = from.entryTime;
		userid = from.userid;
		grainid = from.grainid;
		tablename = from.tablename;
		actionType = from.actionType;
		pkvalue1 = from.pkvalue1;
		pkvalue2 = from.pkvalue2;
		pkvalue3 = from.pkvalue3;
		oldvalues = from.oldvalues;
		newvalues = from.newvalues;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Cursor _getBufferCopy() throws CelestaException {
		// CHECKSTYLE:ON
		LogCursor result = new LogCursor(callContext());
		result.copyFieldsFrom(this);
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _setAutoIncrement(int val) {
		// CHECKSTYLE:ON
		entryno = val;
	}

}
