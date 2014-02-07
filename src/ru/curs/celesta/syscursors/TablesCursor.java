package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице tables.
 * 
 */
public final class TablesCursor extends SysCursor {
	/**
	 * Тип таблицы.
	 */
	public enum TableType {
		/**
		 * Таблица.
		 */
		TABLE, /**
		 * Представление.
		 */
		VIEW
	}

	private String grainid;
	private String tablename;
	private TableType tabletype;
	private boolean orphaned;

	public TablesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	// CHECKSTYLE:OFF
	protected String _tableName() {
		// CHECKSTYLE:ON
		return "tables";
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _parseResult(ResultSet rs) throws SQLException {
		// CHECKSTYLE:ON
		grainid = rs.getString("grainid");
		tablename = rs.getString("tablename");
		tabletype = "T".equals(rs.getString("tabletype")) ? TableType.TABLE
				: TableType.VIEW;
		orphaned = rs.getBoolean("orphaned");
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys) {
			grainid = null;
			tablename = null;
		}
		orphaned = false;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentKeyValues() {
		// CHECKSTYLE:ON
		Object[] result = { grainid, tablename };
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { grainid, tablename,
				tabletype == TableType.TABLE ? "T" : "V", orphaned };
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

	/**
	 * Возвращает тип таблицы: TABLE для таблицы, VIEW - для представления.
	 */
	public TableType getTabletype() {
		return tabletype;
	}

	/**
	 * Устанавливает тип.
	 * 
	 * @param tableType
	 *            Тип: TABLE для таблицы, VIEW - для представления.
	 */
	public void setTabletype(TableType tableType) {
		this.tabletype = tableType;
	}

	@Override
	public void copyFieldsFrom(Cursor c) {
		TablesCursor from = (TablesCursor) c;
		grainid = from.grainid;
		tablename = from.tablename;
		orphaned = from.orphaned;
		tabletype = from.tabletype;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Cursor _getBufferCopy() throws CelestaException {
		// CHECKSTYLE:ON
		TablesCursor result = new TablesCursor(callContext());
		result.copyFieldsFrom(this);
		return result;
	}
}
