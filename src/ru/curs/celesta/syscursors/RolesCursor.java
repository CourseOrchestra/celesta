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
	// CHECKSTYLE:OFF
	protected String _tableName() {
		// CHECKSTYLE:ON
		return "roles";
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _parseResult(ResultSet rs) throws SQLException {
		// CHECKSTYLE:ON
		id = rs.getString("id");
		description = rs.getString("description");
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _clearBuffer(boolean withKeys) {
		// CHECKSTYLE:ON
		if (withKeys)
			id = null;
		description = null;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentKeyValues() {
		// CHECKSTYLE:ON
		Object[] result = { id };
		return result;
	}

	@Override
	// CHECKSTYLE:OFF
	protected Object[] _currentValues() {
		// CHECKSTYLE:ON
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
	// CHECKSTYLE:OFF
	protected Cursor _getBufferCopy() throws CelestaException {
		// CHECKSTYLE:ON
		RolesCursor result = new RolesCursor(callContext());
		result.copyFieldsFrom(this);
		return result;
	}
}
