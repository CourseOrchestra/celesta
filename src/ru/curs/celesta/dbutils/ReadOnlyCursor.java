package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.View;

/**
 * Базовый класс курсора для чтения данных из представлений.
 */
public abstract class ReadOnlyCursor {

	protected static final String SYSTEMUSERID = String.format("SYS%08X",
			(new Random()).nextInt());

	final DBAdaptor db;
	final Connection conn;
	final CallContext context;

	// Поля фильтров и сортировок
	Map<String, AbstractFilter> filters = new HashMap<>();
	String orderBy = null;
	long offset = 0;
	long rowCount = 0;

	public ReadOnlyCursor(CallContext context) throws CelestaException {
		if (context.getConn() == null)
			throw new CelestaException(
					"Invalid context passed to %s constructor: connection is null.",
					this.getClass().getName());
		if (context.getUserId() == null)
			throw new CelestaException(
					"Invalid context passed to %s constructor: user id is null.",
					this.getClass().getName());

		this.context = context;
		conn = context.getConn();
		try {
			if (conn.isClosed())
				throw new CelestaException(
						"Trying to create a cursor on closed connection.");
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		db = DBAdaptor.getAdaptor();
	}
}
