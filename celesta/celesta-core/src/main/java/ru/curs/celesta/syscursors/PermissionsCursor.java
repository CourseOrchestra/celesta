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
 * Курсор на таблице permissions.
 * 
 */
public final class PermissionsCursor extends SysCursor {

	public static final String TABLE_NAME = "permissions";

	private String roleId;
	private String grainId;
	private String tablename;
	private boolean r;
	private boolean i;
	private boolean m;
	private boolean d;

	public PermissionsCursor(CallContext context) throws CelestaException {
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
		roleId = rs.getString("roleid");
		grainId = rs.getString("grainid");
		tablename = rs.getString("tablename");
		r = rs.getBoolean("r");
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
			roleId = null;
			grainId = null;
			tablename = null;
		}
		r = false;
		i = false;
		m = false;
		d = false;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentKeyValues() {
		// CHECKSTYLE:ON
		Object[] result = { roleId, grainId, tablename };
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	public Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { roleId, grainId, tablename, r, i, m, d };
		return result;
	}

	/**
	 * Возвращает идентификатор роли.
	 */
	public String getRoleid() {
		return roleId;
	}

	/**
	 * Устанавливает идентификатор роли.
	 * 
	 * @param roleId
	 *            идентификатор
	 */
	public void setRoleid(String roleId) {
		this.roleId = roleId;
	}

	/**
	 * Возвращает идентификатор гранулы.
	 */
	public String getGrainid() {
		return grainId;
	}

	/**
	 * Устанавливает идентификатор гранулы.
	 * 
	 * @param grainId
	 *            идентификатор
	 */
	public void setGrainid(String grainId) {
		this.grainId = grainId;
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
	 * @param tableName
	 *            Имя таблицы
	 */
	public void setTablename(String tableName) {
		this.tablename = tableName;
	}

	/**
	 * Чтение разрешено.
	 */
	public boolean isR() {
		return r;
	}

	/**
	 * Чтение.
	 * 
	 * @param r
	 *            чтение
	 */
	public void setR(boolean r) {
		this.r = r;
	}

	/**
	 * Вставка разрешена.
	 */
	public boolean isI() {
		return i;
	}

	/**
	 * Вставка.
	 * 
	 * @param i
	 *            вставка
	 */
	public void setI(boolean i) {
		this.i = i;
	}

	/**
	 * Модификация разрешена.
	 */
	public boolean isM() {
		return m;
	}

	/**
	 * Модификация.
	 * 
	 * @param m
	 *            модификация
	 */
	public void setM(boolean m) {
		this.m = m;
	}

	/**
	 * Удаление разрешено.
	 */
	public boolean isD() {
		return d;
	}

	/**
	 * Удаление.
	 * 
	 * @param d
	 *            удаление.
	 */
	public void setD(boolean d) {
		this.d = d;
	}

	@Override
	public void copyFieldsFrom(BasicCursor c) {
		PermissionsCursor from = (PermissionsCursor) c;
		roleId = from.roleId;
		grainId = from.grainId;
		tablename = from.tablename;
		r = from.r;
		i = from.i;
		m = from.m;
		d = from.d;
		setRecversion(from.getRecversion());
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
		// CHECKSTYLE:ON
		PermissionsCursor result = new PermissionsCursor(context);
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
