package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.python.core.PyFunction;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.event.TriggerType;

/**
 * Курсор на таблице logsetup.
 */
public final class LogSetupCursor extends SysCursor {

	public static final String TABLE_NAME = "logsetup";

	private String grainid;
	private String tablename;
	private boolean i;
	private boolean m;
	private boolean d;

	public LogSetupCursor(CallContext context) throws CelestaException {
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
		i = rs.getBoolean("i");
		m = rs.getBoolean("m");
		d = rs.getBoolean("d");
		setRecversion(rs.getInt("recversion"));
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys) {
			grainid = null;
			tablename = null;
		}
		i = false;
		m = false;
		d = false;
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

	@Override
	public void copyFieldsFrom(BasicCursor c) {
		LogSetupCursor from = (LogSetupCursor) c;
		grainid = from.grainid;
		tablename = from.tablename;
		i = from.i;
		m = from.m;
		d = from.d;
		setRecversion(from.getRecversion());
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
		// CHECKSTYLE:ON
		LogSetupCursor result = new LogSetupCursor(context);
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
