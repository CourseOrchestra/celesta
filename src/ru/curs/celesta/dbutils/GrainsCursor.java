package ru.curs.celesta.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

/**
 * Курсор на таблице Grains.
 * 
 */
final class GrainsCursor extends Cursor {

	public static final int READY = 0;
	public static final int UPGRADING = 1;
	public static final int ERROR = 2;
	public static final int RECOVER = 3;

	private String id;
	private String version;
	private int length;
	private int checksum;
	private int state;
	private Date lastmodified;
	private String message;

	public GrainsCursor(CallContext context) throws CelestaException {
		super(context);
	}

	String getId() {
		return id;
	}

	void setId(String id) {
		this.id = id;
	}

	String getVersion() {
		return version;
	}

	void setVersion(String version) {
		this.version = version;
	}

	int getLength() {
		return length;
	}

	void setLength(int length) {
		this.length = length;
	}

	int getChecksum() {
		return checksum;
	}

	void setChecksum(int checksum) {
		this.checksum = checksum;
	}

	int getState() {
		return state;
	}

	void setState(int state) {
		this.state = state;
	}

	Date getLastmodified() {
		return lastmodified;
	}

	void setLastmodified(Date lastmodified) {
		this.lastmodified = lastmodified;
	}

	String getMessage() {
		return message;
	}

	void setMessage(String message) {
		this.message = message;
	}

	@Override
	protected void parseResult(ResultSet rs) throws SQLException {
		id = rs.getString(1);
		version = rs.getString(2);
		length = rs.getInt(3);
		checksum = rs.getInt(4);
		state = rs.getInt(5);
		lastmodified = rs.getDate(6);
		message = rs.getString(7);

	}

	@Override
	protected void clearBuffer(boolean withKeys) {
		if (withKeys)
			id = null;
		version = null;
		length = 0;
		checksum = 0;
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
	protected String grainName() {
		return "celesta";
	}

	@Override
	protected String tableName() {
		return "grains";
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
}
