package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

/**
 * Курсор на таблице permissions.
 * 
 */
public final class PermissionsCursor extends SysCursor {

	private String roleId;
	private String grainId;
	private String tableName;
	private boolean r;
	private boolean i;
	private boolean m;
	private boolean d;

	public PermissionsCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String tableName() {
		return "permissions";
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		roleId = rs.getString("roleid");
		grainId = rs.getString("grainid");
		tableName = rs.getString("tablename");
		r = rs.getBoolean("r");
		i = rs.getBoolean("i");
		m = rs.getBoolean("m");
		d = rs.getBoolean("d");
	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys) {
			roleId = null;
			grainId = null;
			tableName = null;
		}
		r = false;
		i = false;
		m = false;
		d = false;
	}

	@Override
	protected Object[] currentKeyValues() {
		Object[] result = { roleId, grainId, tableName };
		return result;
	}

	@Override
	protected Object[] currentValues() {
		Object[] result = { roleId, grainId, tableName, r, i, m, d };
		return result;
	}

	/**
	 * Возвращает идентификатор роли.
	 */
	public String getRoleId() {
		return roleId;
	}

	/**
	 * Устанавливает идентификатор роли.
	 * 
	 * @param roleId
	 *            идентификатор
	 */
	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

	/**
	 * Возвращает идентификатор гранулы.
	 */
	public String getGrainId() {
		return grainId;
	}

	/**
	 * Устанавливает идентификатор гранулы.
	 * 
	 * @param grainId
	 *            идентификатор
	 */
	public void setGrainId(String grainId) {
		this.grainId = grainId;
	}

	/**
	 * Возвращает имя таблицы.
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Устанавливает имя таблицы.
	 * 
	 * @param tableName
	 *            Имя таблицы
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
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
}
