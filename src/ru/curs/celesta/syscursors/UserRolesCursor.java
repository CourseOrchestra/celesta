package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице userroles.
 */
public final class UserRolesCursor extends SysCursor {

	private String userid;
	private String roleid;

	public UserRolesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	// CHECKSTYLE:OFF
	protected String _tableName() {
		// CHECKSTYLE:ON
		return "userroles";
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _parseResult(ResultSet rs) throws SQLException {
		// CHECKSTYLE:ON
		userid = rs.getString("userid");
		roleid = rs.getString("roleid");
		setRecversion(rs.getInt("recversion"));
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys) {
			userid = null;
			roleid = null;
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentKeyValues() {
		// CHECKSTYLE:ON
		Object[] result = { userid, roleid };
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	public Object[] _currentValues() {
		// CHECKSTYLE:ON
		Object[] result = { userid, roleid };
		return result;
	}

	/**
	 * Идентификатор пользователя.
	 */
	public String getUserid() {
		return userid;
	}

	/**
	 * Устанавливает идентификатор пользователя.
	 * 
	 * @param userId
	 *            идентификатор
	 */
	public void setUserid(String userId) {
		this.userid = userId;
	}

	/**
	 * Роль пользователя.
	 */
	public String getRoleid() {
		return roleid;
	}

	/**
	 * Устанавливает роль пользователя.
	 * 
	 * @param roleId
	 *            имя роли
	 */
	public void setRoleid(String roleId) {
		this.roleid = roleId;
	}

	@Override
	public void copyFieldsFrom(BasicCursor c) {
		UserRolesCursor from = (UserRolesCursor) c;
		userid = from.userid;
		roleid = from.roleid;
		setRecversion(from.getRecversion());
	}

	@Override
	// CHECKSTYLE:OFF
	public Cursor _getBufferCopy(CallContext context) throws CelestaException {
		// CHECKSTYLE:ON
		UserRolesCursor result = new UserRolesCursor(context);
		result.copyFieldsFrom(this);
		return result;
	}
}
