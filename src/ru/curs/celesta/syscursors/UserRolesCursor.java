package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице userroles.
 */
public final class UserRolesCursor extends SysCursor {

	private String userId;
	private String roleId;

	public UserRolesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String tableName() {
		return "userroles";
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		userId = rs.getString("userid");
		roleId = rs.getString("roleid");
	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys) {
			userId = null;
			roleId = null;
		}
	}

	@Override
	protected Object[] currentKeyValues() {
		Object[] result = { userId, roleId };
		return result;
	}

	@Override
	protected Object[] currentValues() {
		Object[] result = { userId, roleId };
		return result;
	}

	/**
	 * Идентификатор пользователя.
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * Устанавливает идентификатор пользователя.
	 * 
	 * @param userId
	 *            идентификатор
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * Роль пользователя.
	 */
	public String getRoleId() {
		return roleId;
	}

	/**
	 * Устанавливает роль пользователя.
	 * 
	 * @param roleId
	 *            имя роли
	 */
	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

	@Override
	public void copyFieldsFrom(Cursor c) {
		UserRolesCursor from = (UserRolesCursor) c;
		userId = from.userId;
		roleId = from.roleId;
	}

	@Override
	protected Cursor getBufferCopy() throws CelestaException {
		UserRolesCursor result = new UserRolesCursor(callContext());
		result.copyFieldsFrom(this);
		return result;
	}
}
