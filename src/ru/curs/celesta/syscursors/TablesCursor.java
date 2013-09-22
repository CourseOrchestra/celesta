package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

/**
 * Курсор на таблице tables.
 * 
 */
public final class TablesCursor extends SysCursor {
	private String grainid;
	private String tablename;
	private boolean orphaned;

	public TablesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String tableName() {
		return "tables";
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		grainid = rs.getString("grainid");
		tablename = rs.getString("tablename");
		orphaned = rs.getBoolean("orphaned");
	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys) {
			grainid = null;
			tablename = null;
		}
		orphaned = false;
	}

	@Override
	protected Object[] currentKeyValues() {
		Object[] result = { grainid, tablename };
		return result;
	}

	@Override
	protected Object[] currentValues() {
		Object[] result = { grainid, tablename, orphaned };
		return result;
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
	 *            Идентификатор гранулы.
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
	 *            имя таблицы
	 */
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	/**
	 * Является ли таблица осиротевшей (отсутствющей в метаданных гранулы).
	 */
	public boolean isOrphaned() {
		return orphaned;
	}

	/**
	 * Устанавливает признак того, что таблица остутствует в метаданных гранулы.
	 * 
	 * @param orphaned
	 *            признак отсутствия в метаданных гранулы.
	 */
	public void setOrphaned(boolean orphaned) {
		this.orphaned = orphaned;
	}

}
