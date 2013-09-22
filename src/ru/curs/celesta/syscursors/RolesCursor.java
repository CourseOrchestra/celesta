package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице roles.
 */
public class RolesCursor extends Cursor {

	private String id;
	private String description;

	public RolesCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	protected String grainName() {
		return "celesta";
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
}
