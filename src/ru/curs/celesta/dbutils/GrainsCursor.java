package ru.curs.celesta.dbutils;

import java.util.Date;

/**
 * Курсор на таблице Grains.
 * 
 */
class GrainsCursor extends AbstractCursor {

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

	public GrainsCursor() {
		// TODO Auto-generated constructor stub
	}

	void get(String id) {

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

}
