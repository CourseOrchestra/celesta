package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.python.core.PyFunction;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.event.TriggerType;

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
		TABLE("T"),
		/**
		 * Представление.
		 */
		VIEW("V"),
		/**
		 * Материализованное представление
		 */
		MATERIALIZED_VIEW("MV"),
		/**
		 * Параметризованное представление
		 */
		FUNCTION("F");

		TableType(String abbreviation) {
			this.abbreviation = abbreviation;
		}

		private String abbreviation;

		private static TableType getByAbbreviation(String abbreviation) {
			return Arrays.stream(values())
					.filter(t -> t.abbreviation.equals(abbreviation))
			    .findFirst().get();
		}
	}

	public static final String TABLE_NAME = "tables";

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
		return TABLE_NAME;
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _parseResult(ResultSet rs) throws SQLException {
		// CHECKSTYLE:ON
		grainid = rs.getString("grainid");
		tablename = rs.getString("tablename");
		tabletype = TableType.getByAbbreviation(rs.getString("tabletype"));
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
	public Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { grainid, tablename,
				tabletype != null ? tabletype.abbreviation : "T", orphaned };
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
	public void copyFieldsFrom(BasicCursor c) {
		TablesCursor from = (TablesCursor) c;
		grainid = from.grainid;
		tablename = from.tablename;
		orphaned = from.orphaned;
		tabletype = from.tabletype;
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
		// CHECKSTYLE:ON
		TablesCursor result = new TablesCursor(context);
		result.copyFieldsFrom(this);
		return result;
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
