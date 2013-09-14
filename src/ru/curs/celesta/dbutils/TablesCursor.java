package ru.curs.celesta.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

/**
 * Курсор на таблице tables.
 * 
 */
final class TablesCursor extends Cursor {
	private String grainid;
	private String tablename;
	private boolean orphaned;

	public TablesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String grainName() {
		return "celesta";
	}

	@Override
	protected String tableName() {
		return "tables";
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		grainid = rs.getString(1);
		tablename = rs.getString(2);
		orphaned = rs.getBoolean(3);
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

	@Override
	protected void preDelete() {
	}

	@Override
	protected void postDelete() {
	}

	@Override
	protected void preUpdate() {
	}

	@Override
	protected void postUpdate() {
	}

	@Override
	protected void preInsert() {
	}

	@Override
	protected void postInsert() {
	}

	public String getGrainid() {
		return grainid;
	}

	public void setGrainid(String grainid) {
		this.grainid = grainid;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public boolean isOrphaned() {
		return orphaned;
	}

	public void setOrphaned(boolean orphaned) {
		this.orphaned = orphaned;
	}

}
