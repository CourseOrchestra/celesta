package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.GrainElement;

/**
 * Базовый класс курсора для чтения данных из представлений.
 */
public abstract class BasicCursor {
	static final String SYSTEMUSERID = String.format("SYS%08X",
			(new Random()).nextInt());
	private static final PermissionManager PERMISSION_MGR = new PermissionManager();

	final DBAdaptor db;
	final Connection conn;
	private final CallContext context;

	// Поля фильтров и сортировок
	Map<String, AbstractFilter> filters = new HashMap<>();
	String orderBy = null;
	long offset = 0;
	long rowCount = 0;

	public BasicCursor(CallContext context) throws CelestaException {
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

	/**
	 * Объект метаданных (таблица или представление), на основе которого создан
	 * данный курсор.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки извлечения метаинформации (в норме не должна
	 *             происходить).
	 */
	public abstract GrainElement meta() throws CelestaException;

	/**
	 * Есть ли у сессии права на чтение текущей таблицы.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canRead() throws CelestaException {
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.READ);
	}

	/**
	 * Есть ли у сессии права на вставку в текущую таблицу.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canInsert() throws CelestaException {
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.INSERT);
	}

	/**
	 * Есть ли у сессии права на модификацию данных текущей таблицы.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canModify() throws CelestaException {
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.MODIFY);
	}

	/**
	 * Есть ли у сессии права на удаление данных текущей таблицы.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canDelete() throws CelestaException {
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.DELETE);
	}

	/**
	 * Возвращает контекст вызова, в котором создан данный курсор.
	 */
	public final CallContext callContext() {
		return context;
	}

	// CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */

	protected abstract String _grainName();

	protected abstract String _tableName();

	protected abstract void _parseResult(ResultSet rs) throws SQLException;

	// CHECKSTYLE:ON

}
