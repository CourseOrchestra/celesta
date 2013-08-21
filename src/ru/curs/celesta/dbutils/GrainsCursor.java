package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;

/**
 * Курсор на таблице Grains.
 * 
 */
class GrainsCursor extends AbstractCursor {

	public static final int READY = 0;
	public static final int UPGRADING = 1;
	public static final int ERROR = 2;
	public static final int RECOVER = 3;

	private Table meta;

	private String id;
	private String version;
	private int length;
	private int checksum;
	private int state;
	private Date lastmodified;
	private String message;

	public GrainsCursor(Connection conn) throws CelestaCritical {
		super(conn);
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
	public Table meta() throws CelestaCritical {
		if (meta == null)
			try {
				meta = Celesta.getInstance().getScore().getGrain("celesta")
						.getTable("grains");
			} catch (ParseException e) {
				throw new CelestaCritical(e.getMessage());
			}
		return meta;
	}

	@Override
	void parseResult(ResultSet rs) throws SQLException {
		id = rs.getString(1);
		version = rs.getString(2);
		length = rs.getInt(3);
		checksum = rs.getInt(4);
		state = rs.getInt(5);
		lastmodified = rs.getDate(6);
		message = rs.getString(7);

	}

	@Override
	void clearBuffer(boolean withKeys) {
		// TODO Auto-generated method stub

	}

	@Override
	Object[] currentKeyValues() {
		Object[] result = { id };
		return result;
	}

	@Override
	Object[] currentValues() {
		Object[] result = { id, version, length, checksum, state, lastmodified,
				message };
		return result;
	}
}
