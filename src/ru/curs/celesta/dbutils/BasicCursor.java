package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.score.CelestaParser;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.ParseException;

/**
 * Базовый класс курсора для чтения данных из представлений.
 */
public abstract class BasicCursor {
	static final String SYSTEMUSERID = String.format("SYS%08X",
			(new Random()).nextInt());
	static final SessionContext SYSTEMSESSION = new SessionContext(
			SYSTEMUSERID, "CELESTA");

	private static final PermissionManager PERMISSION_MGR = new PermissionManager();
	private static final Pattern COLUMN_NAME = Pattern
			.compile("([a-zA-Z_][a-zA-Z0-9_]*)"
					+ "( +([Aa]|[Dd][Ee])[Ss][Cc])?");

	private final DBAdaptor db;
	private final Connection conn;
	private final CallContext context;

	private PreparedStatement set = null;
	private ResultSet cursor = null;

	// Поля фильтров и сортировок
	private final Map<String, AbstractFilter> filters = new HashMap<>();
	private String orderBy = null;
	private long offset = 0;
	private long rowCount = 0;
	private Expr complexFilter;

	public BasicCursor(CallContext context) throws CelestaException {
		if (context == null)
			throw new CelestaException(
					"Invalid context passed to %s constructor: context should not be null.",
					this.getClass().getName());
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

	@Override
	protected void finalize() throws Throwable {
		if (set != null)
			set.close();
	}

	final Map<String, AbstractFilter> getFilters() {
		return filters;
	}

	final Expr getComplexFilterExpr() {
		return complexFilter;
	}

	final DBAdaptor db() {
		return db;
	}

	final Connection conn() {
		return conn;
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

	private void closeSet() throws CelestaException {
		cursor = null;
		if (set != null) {
			try {
				set.close();
			} catch (SQLException e) {
				throw new CelestaException(
						"Database error when closing recordset for table '%s': %s",
						_tableName(), e.getMessage());
			}
			set = null;
		}
	}

	private String getOrderBy() throws CelestaException {
		if (orderBy == null)
			orderBy();
		return orderBy;
	}

	/**
	 * Переходит к первой записи в отфильтрованном наборе и возвращает
	 * информацию об успешности перехода.
	 * 
	 * @return true, если переход успешен, false -- если записей в наборе нет.
	 * 
	 * @throws CelestaException
	 *             Ошибка связи с базой данных
	 */
	public final boolean tryFirst() throws CelestaException {
		if (!canRead())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.READ);

		if (set == null)
			set = db.getRecordSetStatement(conn, meta(), filters,
					complexFilter, getOrderBy(), offset, rowCount);
		boolean result = false;
		try {
			if (cursor != null)
				cursor.close();
			cursor = set.executeQuery();
			result = cursor.next();
			if (result) {
				_parseResult(cursor);
				initXRec();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	void initXRec() throws CelestaException {
	}

	/**
	 * Переходит к первой записи в отфильтрованном наборе, вызывая ошибку в
	 * случае, если переход неудачен.
	 * 
	 * @throws CelestaException
	 *             в случае, если записей в наборе нет.
	 */
	public final void first() throws CelestaException {
		if (!tryFirst()) {
			StringBuilder sb = new StringBuilder();
			for (Entry<String, AbstractFilter> e : filters.entrySet()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(String.format("%s=%s", e.getKey(), e.getValue()
						.toString()));
			}
			throw new CelestaException("There is no %s (%s).", _tableName(),
					sb.toString());
		}
	}

	/**
	 * Возвращает текущее состояние курсора в виде CSV-строки с
	 * разделителями-запятыми.
	 */
	public final String asCSVLine() {
		Object[] values = _currentValues();
		StringBuilder sb = new StringBuilder();
		for (Object value : values) {
			if (sb.length() > 0)
				sb.append(",");
			if (value == null)
				sb.append("NULL");
			else {
				quoteFieldForCSV(value.toString(), sb);
			}
		}
		return sb.toString();
	}

	private static void quoteFieldForCSV(String fieldValue, StringBuilder sb) {
		boolean needQuotes = false;
		for (int i = 0; !needQuotes && i < fieldValue.length(); i++) {
			char c = fieldValue.charAt(i);
			needQuotes = c == '"' || c == ',';
		}
		if (needQuotes) {
			sb.append('"');
			for (int i = 0; i < fieldValue.length(); i++) {
				char c = fieldValue.charAt(i);
				sb.append(c);
				if (c == '"')
					sb.append('"');
			}
			sb.append('"');
		} else {
			sb.append(fieldValue);
		}

	}

	/**
	 * Переходит к следующей записи в отсортированном наборе. Возвращает false,
	 * если достигнут конец набора.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки БД
	 */
	public final boolean next() throws CelestaException {
		boolean result = false;
		try {
			if (cursor == null)
				result = tryFirst();
			else {
				result = cursor.next();
			}
			if (result) {
				_parseResult(cursor);
				initXRec();
			}
		} catch (SQLException e) {
			result = false;
		}
		return result;
	}

	final void validateColumName(String name) throws CelestaException {
		if (!meta().getColumns().containsKey(name))
			throw new CelestaException("No column %s exists in table %s.",
					name, _tableName());
	}

	/**
	 * Сброс любого фильтра на поле.
	 * 
	 * @param name
	 *            Имя поля.
	 * @throws CelestaException
	 *             Неверное имя поля.
	 */
	public final void setRange(String name) throws CelestaException {
		validateColumName(name);
		filters.remove(name);
		closeSet();
	}

	/**
	 * Установка диапазона из единственного значения на поле.
	 * 
	 * @param name
	 *            Имя поля.
	 * @param value
	 *            Значение, по которому осуществляется фильтрация.
	 * @throws CelestaException
	 *             Неверное имя поля
	 */
	public final void setRange(String name, Object value)
			throws CelestaException {
		validateColumName(name);
		filters.put(name, new SingleValue(value));
		closeSet();
	}

	/**
	 * Установка диапазона от..до на поле.
	 * 
	 * @param name
	 *            Имя поля
	 * @param valueFrom
	 *            Значение от
	 * @param valueTo
	 *            Значение до
	 * @throws CelestaException
	 *             Неверное имя поля, SQL-ошибка.
	 */
	public final void setRange(String name, Object valueFrom, Object valueTo)
			throws CelestaException {
		validateColumName(name);
		filters.put(name, new Range(valueFrom, valueTo));
		closeSet();
	}

	/**
	 * Установка фильтра на поле.
	 * 
	 * @param name
	 *            Имя поля
	 * @param value
	 *            Фильтр
	 * @throws CelestaException
	 *             Неверное имя поля и т. п.
	 */
	public final void setFilter(String name, String value)
			throws CelestaException {
		validateColumName(name);
		if (value == null || value.isEmpty())
			throw new CelestaException(
					"Filter for column %s is null or empty. "
							+ "Use setrange(fieldname) to remove any filters from the column.",
					name);
		filters.put(name, new Filter(value));
		closeSet();
	}

	/**
	 * Устанавливает сложное условие на набор данных.
	 * 
	 * @param condition
	 *            Условие, соответствующее выражению where.
	 * @throws CelestaException
	 *             Ошибка разбора выражения.
	 */
	public final void setComplexFilter(String condition)
			throws CelestaException {
		Expr buf = CelestaParser.parseComplexFilter(condition);
		try {
			buf.resolveFieldRefs(meta());
		} catch (ParseException e) {
			throw new CelestaException(e.getMessage());
		}
		complexFilter = buf;
	}

	/**
	 * Возвращает (переформатированное) выражение сложного фильтра в диалекте
	 * CelestaSQL.
	 */
	public final String getComplexFilter() {
		return complexFilter == null ? null : complexFilter.getCSQL();
	}

	/**
	 * Устанавливает фильтр на диапазон возвращаемых курсором записей.
	 * 
	 * @param offset
	 *            Количество записей, которое необходимо пропустить (0 -
	 *            начинать с начала).
	 * @param rowCount
	 *            Максимальное количество записей, которое необходимо вернуть (0
	 *            - вернуть все записи).
	 * @throws CelestaException
	 *             ошибка БД.
	 */
	public final void limit(long offset, long rowCount) throws CelestaException {
		if (offset < 0)
			throw new CelestaException(
					"Negative offset (%d) in limit(...) call", offset);
		if (rowCount < 0)
			throw new CelestaException(
					"Negative rowCount (%d) in limit(...) call", rowCount);
		this.offset = offset;
		this.rowCount = rowCount;
		closeSet();
	}

	/**
	 * Сброс фильтров и сортировки.
	 * 
	 * @throws CelestaException
	 *             SQL-ошибка.
	 */
	public final void reset() throws CelestaException {
		filters.clear();
		complexFilter = null;
		orderBy = null;
		offset = 0;
		rowCount = 0;
		closeSet();
	}

	/**
	 * Установка сортировки.
	 * 
	 * @param names
	 *            Перечень полей для сортировки.
	 * @throws CelestaException
	 *             неверное имя поля или SQL-ошибка.
	 */
	public final void orderBy(String... names) throws CelestaException {
		StringBuilder orderByClause = new StringBuilder();
		boolean needComma = false;
		Set<String> colNames = new HashSet<>();
		for (String name : names) {
			Matcher m = COLUMN_NAME.matcher(name);
			if (!m.matches())
				throw new CelestaException(
						"orderby() argument '%s' should match pattern <column name> [ASC|DESC]",
						name);
			String colName = m.group(1);
			validateColumName(colName);
			if (!colNames.add(colName))
				throw new CelestaException(
						"Column '%s' is used more than once in orderby() call",
						colName);

			String order;
			if (m.group(2) == null || "asc".equalsIgnoreCase(m.group(2).trim())) {
				order = "";
			} else {
				order = " desc";
			}
			if (needComma)
				orderByClause.append(", ");
			orderByClause.append(String.format("\"%s\"%s", colName, order));
			needComma = true;

		}
		appendPK(orderByClause, needComma, colNames);

		orderBy = orderByClause.toString();
		closeSet();
	}

	abstract void appendPK(StringBuilder orderByClause, boolean needComma,
			Set<String> colNames) throws CelestaException;

	/**
	 * Сброс фильтров, сортировки и полная очистка буфера.
	 * 
	 * @throws CelestaException
	 *             SQL-ошибка.
	 */
	public void clear() throws CelestaException {
		_clearBuffer(true);
		filters.clear();
		complexFilter = null;
		orderBy = null;
		offset = 0;
		rowCount = 0;
		closeSet();
	}

	/**
	 * Возвращает число записей в отфильтрованном наборе.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки доступа или ошибки БД
	 */
	public final int count() throws CelestaException {
		int result;
		PreparedStatement stmt = db.getSetCountStatement(conn, meta(), filters,
				complexFilter);
		try {
			ResultSet rs = stmt.executeQuery();
			rs.next();
			result = rs.getInt(1);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				stmt = null;
			}
		}
		return result;
	}

	/**
	 * Получает копию фильтров, а также значений limit (offset и rowcount) из
	 * курсора того же типа.
	 * 
	 * @param c
	 *            Курсор, фильтры которого нужно скопировать.
	 * @throws CelestaException
	 *             неверный тип курсора
	 */
	public final void copyFiltersFrom(BasicCursor c) throws CelestaException {
		if (!(c._grainName().equals(_grainName()) && c._tableName().equals(
				_tableName())))
			throw new CelestaException(
					"Cannot assign filters from cursor for %s.%s to cursor for %s.%s.",
					c._grainName(), c._tableName(), _grainName(), _tableName());
		filters.clear();
		filters.putAll(c.filters);
		complexFilter = c.complexFilter;
		offset = c.offset;
		rowCount = c.rowCount;
		closeSet();
	}

	/**
	 * Получает копию сортировок из курсора того же типа.
	 * 
	 * @param c
	 *            Курсор, фильтры которого нужно скопировать.
	 * @throws CelestaException
	 *             неверный тип курсора
	 */
	public final void copyOrderFrom(BasicCursor c) throws CelestaException {
		if (!(c._grainName().equals(_grainName()) && c._tableName().equals(
				_tableName())))
			throw new CelestaException(
					"Cannot assign ordering from cursor for %s.%s to cursor for %s.%s.",
					c._grainName(), c._tableName(), _grainName(), _tableName());
		orderBy = c.orderBy;
		closeSet();
	}

	// CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */
	protected abstract void _clearBuffer(boolean withKeys);

	protected abstract String _grainName();

	protected abstract String _tableName();

	protected abstract void _parseResult(ResultSet rs) throws SQLException;

	protected abstract Object[] _currentValues();
	// CHECKSTYLE:ON

}
