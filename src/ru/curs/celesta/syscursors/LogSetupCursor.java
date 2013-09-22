package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

/**
 * Курсор на таблице logsetup.
 */
public class LogSetupCursor extends SysCursor {

	private String grainid;
	private String tablename;
	private boolean i;
	private boolean m;
	private boolean d;

	public LogSetupCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String tableName() {
		return "logsetup";
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		grainid = rs.getString("grainid");
		tablename = rs.getString("tablename");
		i = rs.getBoolean("i");
		m = rs.getBoolean("m");
		d = rs.getBoolean("d");
	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys) {
			grainid = null;
			tablename = null;
		}
		i = false;
		m = false;
		d = false;
	}

	@Override
	protected Object[] currentKeyValues() {
		Object[] result = { grainid, tablename };
		return result;
	}

	@Override
	protected Object[] currentValues() {
		Object[] result = { grainid, tablename, i, m, d };
		return result;
	}

	/**
	 * Возвращает имя гранулы.
	 */
	public String getGrainid() {
		return grainid;
	}

	/**
	 * Устанавливает имя гранулы.
	 * 
	 * @param grainid
	 *            имя гранулы
	 */
	public void setGrainid(String grainid) {
		this.grainid = grainid;
	}

	/**
	 * Возвращает имя таблицы.
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * Устанавливает имя таблицы.
	 * 
	 * @param tablename
	 *            имя таблицы
	 * 
	 */
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	/**
	 * Логировать вставку.
	 */
	public boolean isI() {
		return i;
	}

	/**
	 * Логировать вставку.
	 * 
	 * @param i
	 *            логировать
	 */
	public void setI(boolean i) {
		this.i = i;
	}

	/**
	 * Логировать модификацию.
	 */
	public boolean isM() {
		return m;
	}

	/**
	 * Логировать модификацию.
	 * 
	 * @param m
	 *            логировать
	 */
	public void setM(boolean m) {
		this.m = m;
	}

	/**
	 * Логировать удаление.
	 */
	public boolean isD() {
		return d;
	}

	/**
	 * Логировать удаление.
	 * 
	 * @param d
	 *            логировать
	 */
	public void setD(boolean d) {
		this.d = d;
	}

}
