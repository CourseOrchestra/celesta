/*
   (с) 2013 ООО "КУРС-ИТ"  

   Этот файл — часть КУРС:Celesta.
   
   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.

   
   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */

package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

	static final String SYSTEMUSERID = String.format("SYS%08X", (new Random()).nextInt());
	static final SessionContext SYSTEMSESSION = new SessionContext(SYSTEMUSERID, "CELESTA");

	private static final String DATABASE_CLOSING_ERROR = "Database error when closing recordset for table '%s': %s";
	private static final String CURSOR_IS_CLOSED = "Cursor is closed.";

	private static final PermissionManager PERMISSION_MGR = new PermissionManager();
	private static final Pattern COLUMN_NAME = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)( +([Aa]|[Dd][Ee])[Ss][Cc])?");

	private static final Pattern NAVIGATION = Pattern.compile("[+-<>=]+");
	private final DBAdaptor db;
	private final Connection conn;
	private final CallContext context;

	private PreparedStatement set = null;
	private ResultSet cursor = null;

	private PreparedStatement forwards = null;
	private PreparedStatement backwards = null;
	private PreparedStatement here = null;
	private PreparedStatement first = null;
	private PreparedStatement last = null;

	// Поля фильтров и сортировок
	private final Map<String, AbstractFilter> filters = new HashMap<>();
	private String[] orderByNames;
	private boolean[] descOrders;

	private long offset = 0;
	private long rowCount = 0;
	private Expr complexFilter;
	private boolean closed = false;

	private BasicCursor previousCursor;
	private BasicCursor nextCursor;

	public BasicCursor(CallContext context) throws CelestaException {
		if (context == null)
			throw new CelestaException("Invalid context passed to %s constructor: context should not be null.",
					this.getClass().getName());
		if (context.getConn() == null)
			throw new CelestaException("Invalid context passed to %s constructor: connection is null.",
					this.getClass().getName());
		if (context.getUserId() == null)
			throw new CelestaException("Invalid context passed to %s constructor: user id is null.",
					this.getClass().getName());
		context.incCursorCount();

		this.context = context;
		previousCursor = context.getLastCursor();
		if (previousCursor != null)
			previousCursor.nextCursor = this;
		context.setLastCursor(this);

		conn = context.getConn();
		try {
			if (conn.isClosed())
				throw new CelestaException("Trying to create a cursor on closed connection.");
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		db = DBAdaptor.getAdaptor();
	}

	final void close(PreparedStatement... stmts) {
		closed = true;
		for (PreparedStatement stmt : stmts) {
			try {
				if (!(stmt == null || stmt.isClosed()))
					stmt.close();
			} catch (SQLException e) {
				e = null;
			}
		}
	}

	/**
	 * Закрывает курсор, высвобождает все его PreparedStatements и делает курсор
	 * невозможным к дальнейшему использованию.
	 */
	public void close() {
		if (!closed) {
			if (this == context.getLastCursor())
				context.setLastCursor(previousCursor);
			if (previousCursor != null)
				previousCursor.nextCursor = nextCursor;
			if (nextCursor != null)
				nextCursor.previousCursor = previousCursor;
			context.decCursorCount();
			close(set, forwards, backwards, here, first, last);
		}
	}

	@Override
	protected void finalize() throws Throwable {
		close(set, forwards, backwards, here, first, last);
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
		if (closed)
			throw new CelestaException(CURSOR_IS_CLOSED);
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.READ);
	}

	/**
	 * Есть ли у сессии права на вставку в текущую таблицу.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canInsert() throws CelestaException {
		if (closed)
			throw new CelestaException(CURSOR_IS_CLOSED);
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.INSERT);
	}

	/**
	 * Есть ли у сессии права на модификацию данных текущей таблицы.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canModify() throws CelestaException {
		if (closed)
			throw new CelestaException(CURSOR_IS_CLOSED);
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.MODIFY);
	}

	/**
	 * Есть ли у сессии права на удаление данных текущей таблицы.
	 * 
	 * @throws CelestaException
	 *             ошибка базы данных.
	 */
	public final boolean canDelete() throws CelestaException {
		if (closed)
			throw new CelestaException(CURSOR_IS_CLOSED);
		return PERMISSION_MGR.isActionAllowed(context, meta(), Action.DELETE);
	}

	/**
	 * Возвращает контекст вызова, в котором создан данный курсор.
	 */
	public final CallContext callContext() {
		return context;
	}

	private void closeStmt(PreparedStatement stmt) throws CelestaException {
		try {
			stmt.close();
		} catch (SQLException e) {
			throw new CelestaException(DATABASE_CLOSING_ERROR, _tableName(), e.getMessage());
		}
	}

	private void closeSet() throws CelestaException {
		cursor = null;
		if (set != null) {
			closeStmt(set);
			set = null;
		}
		if (forwards != null) {
			closeStmt(forwards);
			forwards = null;
		}
		if (backwards != null) {
			closeStmt(backwards);
			backwards = null;
		}
		if (first != null) {
			closeStmt(first);
			first = null;
		}
		if (last != null) {
			closeStmt(last);
			last = null;
		}
	}

	private String getOrderBy(boolean reverse) throws CelestaException {
		if (orderByNames == null)
			orderBy();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < orderByNames.length; i++) {
			if (i > 0)
				sb.append(", ");
			sb.append(orderByNames[i]);
			if (reverse ^ descOrders[i])
				sb.append(" desc");
		}
		return sb.toString();

	}

	String getOrderBy() throws CelestaException {
		return getOrderBy(false);
	}

	String getReversedOrderBy() throws CelestaException {
		return getOrderBy(true);
	}

	String[] orderByColumnNames() throws CelestaException {
		if (orderByNames == null)
			orderBy();
		return orderByNames;
	}

	boolean[] descOrders() throws CelestaException {
		if (orderByNames == null)
			orderBy();
		return descOrders;
	}

	String getNavigationWhereClause(char op) throws CelestaException {
		boolean invert = false;
		if (orderByNames == null)
			orderBy();
		switch (op) {
		case '>':
			invert = false;
			break;
		case '<':
			invert = true;
			break;
		case '=':
			StringBuilder sb = new StringBuilder("(");
			for (int i = 0; i < orderByNames.length; i++) {
				if (i > 0)
					sb.append(" and ");
				sb.append(orderByNames[i]);
				sb.append(" = ?");
			}
			sb.append(")");
			return sb.toString();
		default:
			throw new CelestaException("Invalid navigation operator: %s", op);
		}

		char[] ops = new char[orderByNames.length];
		for (int i = 0; i < orderByNames.length; i++) {
			ops[i] = (invert ^ descOrders[i]) ? '<' : '>';
		}
		return getNavigationWhereClause(0, ops);
	}

	private String getNavigationWhereClause(int i, char[] ops) {
		String result;
		if (orderByNames.length - 1 > i) {
			// This is logically equivalent to the 'obvious' formula, but runs
			// much faster on Postgres (apparently because of conjunction as a
			// top logical operator)
			result = String.format("((%s %s= ?) and ((%s %s ?) or %s))", orderByNames[i], ops[i], orderByNames[i],
					ops[i], getNavigationWhereClause(i + 1, ops));
		} else {
			result = String.format("(%s %s ?)", orderByNames[i], ops[i]);
		}
		return result;
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
	public final boolean tryFindSet() throws CelestaException {
		if (!canRead())
			throw new PermissionDeniedException(callContext(), meta(), Action.READ);

		if (isNotValid(set))
			set = db.getRecordSetStatement(conn, meta(), filters, complexFilter, getOrderBy(), offset, rowCount);
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

	/**
	 * То же, что navigate("-").
	 * 
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public final boolean tryFirst() throws CelestaException {
		return navigate("-");
	}

	/**
	 * То же, что tryFirst(), но вызывает ошибку, если запись не найдена.
	 * 
	 * @throws CelestaException
	 *             Запись не найдена или ошибка БД.
	 */
	public final void first() throws CelestaException {
		if (!navigate("-"))
			raiseNotFound();
	}

	/**
	 * То же, что navigate("+").
	 * 
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public final boolean tryLast() throws CelestaException {
		return navigate("+");
	}

	/**
	 * То же, что tryLast(), но вызывает ошибку, если запись не найдена.
	 * 
	 * @throws CelestaException
	 *             Запись не найдена или ошибка БД.
	 */
	public final void last() throws CelestaException {
		if (!navigate("+"))
			raiseNotFound();
	}

	/**
	 * То же, что navigate("&gt;").
	 * 
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public final boolean next() throws CelestaException {
		return navigate(">");
	}

	/**
	 * То же, что navigate("&lt").
	 * 
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public final boolean previous() throws CelestaException {
		return navigate("<");
	}

	private void raiseNotFound() throws CelestaException {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, AbstractFilter> e : filters.entrySet()) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(String.format("%s=%s", e.getKey(), e.getValue().toString()));
		}
		throw new CelestaException("There is no %s (%s).", _tableName(), sb.toString());
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
	public final void findSet() throws CelestaException {
		if (!tryFindSet())
			raiseNotFound();
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
	public final boolean nextInSet() throws CelestaException {
		boolean result = false;
		try {
			if (cursor == null)
				result = tryFindSet();
			else {
				result = cursor.next();
			}
			if (result) {
				_parseResult(cursor);
				initXRec();
			} else {
				cursor.close();
				cursor = null;
			}
		} catch (SQLException e) {
			result = false;
		}
		return result;
	}

	/**
	 * Метод навигации (пошагового перехода в отфильтрованном и отсортированном
	 * наборе).
	 * 
	 * @param command
	 *            Команда, состоящая из последовательности символов:
	 *            <ul>
	 *            <li>= обновить текущую запись (если она имеется в
	 *            отфильтрованном наборе)</li>
	 *            <li>&gt; перейти к следующей записи в отфильтрованном наборе
	 *            </li>
	 *            <li>&lt; перейти к предыдущей записи в отфильтрованном наборе
	 *            </li>
	 *            <li>- перейти к первой записи в отфильтрованном наборе</li>
	 *            <li>+ перейти к последней записи в отфильтрованном наборе</li>
	 *            </ul>
	 * @return true, если запись найдена и переход совершился, false — в
	 *         противном случае.
	 * @throws CelestaException
	 *             некорректный формат команды или сбой при работе с БД.
	 */
	public boolean navigate(String command) throws CelestaException {
		if (!canRead())
			throw new PermissionDeniedException(callContext(), meta(), Action.READ);

		Matcher m = NAVIGATION.matcher(command);
		if (!m.matches())
			throw new CelestaException(
					"Invalid navigation command: '%s', should consist of '+', '-', '>', '<' and '=' only!", command);
		Object[] valsArray = _currentValues();

		Map<String, Object> valsMap = new HashMap<>();
		int j = 0;
		for (String colName : meta().getColumns().keySet())
			valsMap.put('"' + colName + '"', valsArray[j++]);
		for (int i = 0; i < command.length(); i++) {
			char c = command.charAt(i);
			PreparedStatement navigator = chooseNavigator(c);
			j = db().fillSetQueryParameters(filters, navigator);
			fillNavigationParams(navigator, valsMap, j, c);
			try {
				// System.out.println(navigator);
				ResultSet rs = navigator.executeQuery();
				try {
					if (rs.next()) {
						_parseResult(rs);
						initXRec();
						return true;
					}
				} finally {
					rs.close();
				}
			} catch (SQLException e) {
				throw new CelestaException("Error while navigating cursor: %s", e.getMessage());
			}
		}
		return false;
	}

	void fillNavigationParams(PreparedStatement navigator, Map<String, Object> valuesMap, int k, char c)
			throws CelestaException {
		if (c == '-' || c == '+')
			return;

		int j = k;

		if (orderByNames == null)
			orderBy();
		for (int i = 0; i < orderByNames.length; i++) {
			Object value = valuesMap.get(orderByNames[i]);
			// System.out.printf("%d = %s%n", j, value);
			DBAdaptor.setParam(navigator, j++, value);
			if (c != '=' && i < orderByNames.length - 1) {
				// System.out.printf("%d = %s%n", j, value);
				DBAdaptor.setParam(navigator, j++, value);
			}
		}
	}

	private static boolean isNotValid(PreparedStatement stmt) throws CelestaException {
		try {
			return stmt == null || stmt.isClosed();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	private PreparedStatement chooseNavigator(char c) throws CelestaException {
		switch (c) {
		case '<':
			if (isNotValid(backwards))
				backwards = db().getNavigationStatement(conn, meta(), filters, complexFilter, getReversedOrderBy(),
						getNavigationWhereClause('<'));
			return backwards;
		case '>':
			if (isNotValid(forwards))
				forwards = db().getNavigationStatement(conn, meta(), filters, complexFilter, getOrderBy(),
						getNavigationWhereClause('>'));
			return forwards;
		case '=':
			if (isNotValid(here))
				here = db().getNavigationStatement(conn, meta(), filters, complexFilter, getOrderBy(),
						getNavigationWhereClause('='));
			return here;
		case '-':
			if (isNotValid(first))
				first = db().getNavigationStatement(conn, meta(), filters, complexFilter, getOrderBy(), "");
			return first;
		case '+':
			if (isNotValid(last))
				last = db().getNavigationStatement(conn, meta(), filters, complexFilter, getReversedOrderBy(), "");
			return last;
		default:
			// THIS WILL NEVER EVER HAPPEN, WE'VE ALREADY CHECKED
			return null;
		}

	}

	final void validateColumName(String name) throws CelestaException {
		if (!meta().getColumns().containsKey(name))
			throw new CelestaException("No column %s exists in table %s.", name, _tableName());
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
		if (closed)
			return;
		// Если фильтр присутствовал на поле -- сбрасываем набор. Если не
		// присутствовал -- не сбрасываем.
		if (filters.remove(name) != null)
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
	public final void setRange(String name, Object value) throws CelestaException {
		if (value == null) {
			setFilter(name, "null");
		} else {
			validateColumName(name);
			AbstractFilter oldFilter = filters.put(name, new SingleValue(value));
			if (closed)
				return;
			// Если один SingleValue меняется на другой SingleValue -- то
			// необязательно закрывать набор, можно использовать старый.
			if (oldFilter instanceof SingleValue) {
				if (!isNotValid(set))
					db().fillSetQueryParameters(filters, set);
			} else {
				closeSet();
			}
		}
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
	public final void setRange(String name, Object valueFrom, Object valueTo) throws CelestaException {
		validateColumName(name);
		AbstractFilter oldFilter = filters.put(name, new Range(valueFrom, valueTo));
		if (closed)
			return;
		// Если один Range меняется на другой Range -- то
		// необязательно закрывать набор, можно использовать старый.
		if (oldFilter instanceof Range) {
			if (!isNotValid(set))
				db().fillSetQueryParameters(filters, set);
		} else {
			closeSet();
		}
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
	public final void setFilter(String name, String value) throws CelestaException {
		validateColumName(name);
		if (value == null || value.isEmpty())
			throw new CelestaException("Filter for column %s is null or empty. "
					+ "Use setrange(fieldname) to remove any filters from the column.", name);
		AbstractFilter oldFilter = filters.put(name, new Filter(value));
		if (closed)
			return;
		// Если заменили фильтр на тот же самый -- ничего делать не надо.
		if (!(oldFilter instanceof Filter && value.equals(oldFilter.toString())))
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
	public final void setComplexFilter(String condition) throws CelestaException {
		Expr buf = CelestaParser.parseComplexFilter(condition);
		try {
			buf.resolveFieldRefs(meta());
		} catch (ParseException e) {
			throw new CelestaException(e.getMessage());
		}
		complexFilter = buf;
		if (closed)
			return;
		// пересоздаём набор
		closeSet();
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
			throw new CelestaException("Negative offset (%d) in limit(...) call", offset);
		if (rowCount < 0)
			throw new CelestaException("Negative rowCount (%d) in limit(...) call", rowCount);
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
		orderByNames = null;
		descOrders = null;
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

		ArrayList<String> l = new ArrayList<>(8);
		ArrayList<Boolean> ol = new ArrayList<>(8);
		Set<String> colNames = new HashSet<>();
		for (String name : names) {
			Matcher m = COLUMN_NAME.matcher(name);
			if (!m.matches())
				throw new CelestaException("orderby() argument '%s' should match pattern <column name> [ASC|DESC]",
						name);
			String colName = m.group(1);
			validateColumName(colName);
			if (!colNames.add(colName))
				throw new CelestaException("Column '%s' is used more than once in orderby() call", colName);

			boolean order = !(m.group(2) == null || "asc".equalsIgnoreCase(m.group(2).trim()));

			l.add(String.format("\"%s\"", colName));
			ol.add(order);
		}

		appendPK(l, ol, colNames);

		orderByNames = new String[l.size()];
		descOrders = new boolean[l.size()];
		for (int i = 0; i < orderByNames.length; i++) {
			orderByNames[i] = l.get(i);
			descOrders[i] = ol.get(i);
		}
		closeSet();
	}

	abstract void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) throws CelestaException;

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
		orderByNames = null;
		descOrders = null;
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
		PreparedStatement stmt = db.getSetCountStatement(conn, meta(), filters, complexFilter);
		return count(stmt);
	}

	private int count(PreparedStatement stmt) throws CelestaException {
		int result;
		try {
			ResultSet rs = stmt.executeQuery();
			rs.next();
			result = rs.getInt(1);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		} finally {
			closeStmt(stmt);
		}
		return result;
	}

	/**
	 * A hidden method that returns total count of rows that precede the current
	 * one in the set. This method is intended for internal use by GridDriver.
	 * Since rows counting is a resource-consuming operation, this method should
	 * not be public.
	 */
	final int position() throws CelestaException {
		PreparedStatement stmt = db.getSetCountStatement(conn, meta(), filters, complexFilter,
				getNavigationWhereClause('<'));

		// getting navigation clause params
		Map<String, Object> valsMap = new HashMap<>();
		int j = 0;
		Object[] vals = _currentValues();
		for (String colName : meta().getColumns().keySet())
			valsMap.put("\"" + colName + "\"", vals[j++]);

		j = db.fillSetQueryParameters(filters, stmt);
		// System.out.println(stmt.toString());

		if (orderByNames == null)
			orderBy();

		for (int i = 0; i < orderByNames.length; i++) {
			Object param = valsMap.get(orderByNames[i]);
			DBAdaptor.setParam(stmt, j++, param);
			if (i < orderByNames.length - 1) {
				DBAdaptor.setParam(stmt, j++, param);
			}
		}
		// System.out.println(stmt);
		return count(stmt);
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
		if (!(c._grainName().equals(_grainName()) && c._tableName().equals(_tableName())))
			throw new CelestaException("Cannot assign filters from cursor for %s.%s to cursor for %s.%s.",
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
		if (!(c._grainName().equals(_grainName()) && c._tableName().equals(_tableName())))
			throw new CelestaException("Cannot assign ordering from cursor for %s.%s to cursor for %s.%s.",
					c._grainName(), c._tableName(), _grainName(), _tableName());
		orderByNames = c.orderByNames;
		descOrders = c.descOrders;
		closeSet();
	}

	/**
	 * Устанавливает значение поля по его имени. Необходимо для косвенного
	 * заполнения данными курсора из Java (в Python, естественно, для этой цели
	 * есть процедура setattr(...)).
	 * 
	 * @param name
	 *            Имя поля.
	 * @param value
	 *            Значение поля.
	 * @throws CelestaException
	 *             Если поле не найдено по имени.
	 */
	public final void setValue(String name, Object value) throws CelestaException {
		validateColumName(name);
		_setFieldValue(name, value);
	}

	/**
	 * Является ли курсор закрытым.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Копировать значения полей из курсора того же типа.
	 * 
	 * @param from
	 *            курсор, из которого следует скопировать значения полей
	 */
	public abstract void copyFieldsFrom(BasicCursor from);

	// CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */
	public abstract BasicCursor _getBufferCopy(CallContext context) throws CelestaException;

	public abstract Object[] _currentValues();

	protected abstract void _clearBuffer(boolean withKeys);

	protected abstract String _grainName();

	protected abstract String _tableName();

	protected abstract void _parseResult(ResultSet rs) throws SQLException;

	protected abstract void _setFieldValue(String name, Object value);

	// CHECKSTYLE:ON

}
