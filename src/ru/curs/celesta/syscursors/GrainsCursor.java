package ru.curs.celesta.syscursors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Курсор на таблице Grains.
 * 
 */
public final class GrainsCursor extends SysCursor {

	/**
	 * Статус "готов".
	 */
	public static final int READY = 0;
	/**
	 * Статус "в процессе обновления".
	 */
	public static final int UPGRADING = 1;
	/**
	 * Статус "ошибка".
	 */
	public static final int ERROR = 2;
	/**
	 * Статус "обновить!".
	 */
	public static final int RECOVER = 3;

	private String id;
	private String version;
	private int length;
	private String checksum;
	private int state;
	private Date lastmodified;
	private String message;

	public GrainsCursor(CallContext context) throws CelestaException {
		super(context);
	}

	/**
	 * Идентификатор.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Установить идентификатор.
	 * 
	 * @param id
	 *            идентификатор
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Версия.
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * Устанавливает версию.
	 * 
	 * @param version
	 *            версия
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * Длина скрипта гранулы.
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Устанавливает длину скрипта гранулы.
	 * 
	 * @param length
	 *            длина
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * Контрольная сумма скрипта гранулы.
	 */
	public String getChecksum() {
		return checksum;
	}

	/**
	 * Устанавливает контрольную сумму скрипта гранулы.
	 * 
	 * @param checksum
	 *            контрольная сумма.
	 */
	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}

	/**
	 * Статус гранулы.
	 */
	public int getState() {
		return state;
	}

	/**
	 * Устанавливает статус гранулы.
	 * 
	 * @param state
	 *            статус.
	 */
	public void setState(int state) {
		this.state = state;
	}

	/**
	 * Дата последнего обновления в базе данных по данной грануле.
	 */
	public Date getLastmodified() {
		return lastmodified;
	}

	/**
	 * Устанавливает дату посоледнего обновления.
	 * 
	 * @param lastmodified
	 *            Дата последнего обновления в базе данных по данной грануле.
	 */
	public void setLastmodified(Date lastmodified) {
		this.lastmodified = lastmodified;
	}

	/**
	 * Сообщение после последнего обновления.
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * Устанавливает сообщение после последнего обновления.
	 * 
	 * @param message
	 *            сообщение
	 */
	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		id = rs.getString("id");
		version = rs.getString("version");
		length = rs.getInt("length");
		checksum = rs.getString("checksum");
		state = rs.getInt("state");
		lastmodified = rs.getDate("lastmodified");
		message = rs.getString("message");

	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys)
			id = null;
		version = null;
		length = 0;
		checksum = "";
		state = 0;
		lastmodified = null;
		message = null;
	}

	@Override
	protected Object[] currentKeyValues() {
		Object[] result = { id };
		return result;
	}

	@Override
	protected Object[] currentValues() {
		Object[] result = { id, version, length, checksum, state, lastmodified,
				message };
		return result;
	}

	@Override
	protected String tableName() {
		return "grains";
	}

	@Override
	public void copyFieldsFrom(Cursor c) {
		GrainsCursor from = (GrainsCursor) c;
		id = from.id;
		version = from.version;
		length = from.length;
		checksum = from.checksum;
		state = from.state;
		lastmodified = from.lastmodified;
		message = from.message;
	}

	@Override
	protected Cursor getBufferCopy() throws CelestaException {
		GrainsCursor result = new GrainsCursor(callContext());
		result.copyFieldsFrom(this);
		return result;
	}
}
