package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице roles.
 */
public final class RolesCursor extends SysCursor {

	private String id;
	private String description;

	public RolesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String tableName() {
		return "roles";
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		id = rs.getString("id");
		description = rs.getString("description");
	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys)
			id = null;
		description = null;
	}

	@Override
	protected Object[] currentKeyValues() {
		Object[] result = { id };
		return result;
	}

	@Override
	protected Object[] currentValues() {
		Object[] result = { id, description };
		return result;
	}

	/**
	 * Возвращает целочисленный идентификатор роли.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Устанавливает идентификатор роли.
	 * 
	 * @param id
	 *            новый идентификатор
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Возвращает описание (имя) роли.
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Устанавливает новое описание роли.
	 * 
	 * @param description
	 *            описание роли.
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public void copyFieldsFrom(Cursor c) {
		RolesCursor from = (RolesCursor) c;
		id = from.id;
		description = from.description;
	}

	@Override
	protected Cursor getBufferCopy() throws CelestaException {
		RolesCursor result = new RolesCursor(callContext());
		result.copyFieldsFrom(this);
		return result;
	}
}
