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

import java.io.Closeable;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.*;
import java.util.stream.Collectors;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.filter.*;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.*;
import ru.curs.celesta.dbutils.term.*;
import ru.curs.celesta.score.*;
import ru.curs.celesta.score.ParseException;

/**
 * Базовый класс курсора для чтения данных из представлений.
 */
public abstract class BasicCursor implements Closeable {

	static final String SYSTEMUSERID = String.format("SYS%08X", (new Random()).nextInt());
	static final SessionContext SYSTEMSESSION = new SessionContext(SYSTEMUSERID, "CELESTA");

	private static final String DATABASE_CLOSING_ERROR =
		"Database error when closing recordset for table '%s': %s";
	private static final String CURSOR_IS_CLOSED = "Cursor is closed.";
	private static final String NAVIGATING_ERROR = "Error while navigating cursor: %s";

	private static final Pattern COLUMN_NAME =
		Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)( +([Aa]|[Dd][Ee])[Ss][Cc])?");

	private static final Pattern NAVIGATION = Pattern.compile("[+-<>=]+");
	private static final Pattern NAVIGATION_WITH_OFFSET = Pattern.compile("[<>]");

	private final DBAdaptor db;
	private final Connection conn;
	private final CallContext context;
	protected Set<String> fields = Collections.emptySet();
	protected Set<String> fieldsForStatement = Collections.emptySet();

	private final PreparedStmtHolder set = new PreparedStmtHolder() {
		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm();
			fromTerm.programParams(program);
			where.programParams(program);
			return db.getRecordSetStatement(conn, getFrom(), where.getWhere(), getOrderBy(), offset,
					rowCount, fieldsForStatement);
		}
	};

	private ResultSet cursor = null;

	private final PreparedStmtHolder count = new PreparedStmtHolder() {
		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm();

			fromTerm.programParams(program);
			where.programParams(program);
			return db.getSetCountStatement(conn, from, where.getWhere());
		}
	};

	/**
	 * Base holder class for a number of queries that depend on null mask on
	 * sort fields.
	 */
	abstract class OrderFieldsMaskedStatementHolder extends MaskedStatementHolder {
		@Override
		protected final int[] getNullsMaskIndices() throws CelestaException {
			if (orderByNames == null)
				orderBy();
			return orderByIndices;
		}
	}

	private final PreparedStmtHolder position = new OrderFieldsMaskedStatementHolder() {
		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm('<');
			fromTerm.programParams(program);
			where.programParams(program);
			return db.getSetCountStatement(conn, getFrom(), where.getWhere());
		}

	};

	private final PreparedStmtHolder forwards = new OrderFieldsMaskedStatementHolder() {
		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm('>');
			fromTerm.programParams(program);
			where.programParams(program);
			return db.getNavigationStatement(
					conn, getFrom(), getOrderBy(), where.getWhere(), fieldsForStatement, navigationOffset
			);
		}

	};
	private final PreparedStmtHolder backwards = new OrderFieldsMaskedStatementHolder() {

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm('<');
			fromTerm.programParams(program);
			where.programParams(program);
			return db.getNavigationStatement(
					conn, getFrom(), getReversedOrderBy(), where.getWhere(), fieldsForStatement, navigationOffset
			);
		}

	};

	private final PreparedStmtHolder here = getHereHolder();

	private final PreparedStmtHolder first = new PreparedStmtHolder() {

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm();
			fromTerm.programParams(program);
			where.programParams(program);
			return db.getNavigationStatement(
					conn, getFrom(), getOrderBy(), where.getWhere(), fieldsForStatement, 0
			);
		}

	};
	private final PreparedStmtHolder last = new PreparedStmtHolder() {
		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program)
				throws CelestaException {
			FromClause from = getFrom();

			if (fromTerm == null) {
				fromTerm = new FromTerm(from.getParameters());
			}

			WhereTerm where = qmaker.getWhereTerm();
			fromTerm.programParams(program);
			where.programParams(program);
			return db.getNavigationStatement(
					conn, getFrom(), getReversedOrderBy(), where.getWhere(), fieldsForStatement, 0
			);
		}
	};

	// Поля фильтров и сортировок
	private final Map<String, AbstractFilter> filters = new HashMap<>();
	private String[] orderByNames;
	private int[] orderByIndices;
	private boolean[] descOrders;

	private long offset = 0;
	private long navigationOffset = 0;
	private long rowCount = 0;
	private Expr complexFilter;

	private boolean closed = false;

	private BasicCursor previousCursor;
	private BasicCursor nextCursor;

	protected FromTerm fromTerm;

	private final WhereTermsMaker qmaker = new WhereTermsMaker(new WhereMakerParamsProvider() {

		@Override
		public void initOrderBy() throws CelestaException {
			if (orderByNames == null)
				orderBy();
		}

		@Override
		public QueryBuildingHelper dba() {
			return db;
		}

		@Override
		public String[] sortFields() throws CelestaException {
			return orderByNames;
		}

		@Override
		public boolean[] descOrders() throws CelestaException {
			return descOrders;
		}

		@Override
		public Map<String, AbstractFilter> filters() {
			return filters;
		}

		@Override
		public Expr complexFilter() {
			return complexFilter;
		}

		@Override
		public In inFilter() {
			return getIn();
		}

		@Override
		public int[] sortFieldsIndices() throws CelestaException {
			return orderByIndices;
		}

		@Override
		public Object[] values() throws CelestaException {
			return _currentValues();
		}

		@Override
		public boolean isNullable(String columnName) throws CelestaException {
			return meta().getColumns().get(columnName).isNullable();
		}
	});

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
		if (context.isClosed())
			throw new CelestaException("Cannot create %s on a closed CallContext.",
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
		db = callContext().getDbAdaptor();
	}

	public BasicCursor(CallContext context, Set<String> fields) throws CelestaException {
		this(context);
		if (!meta().getColumns().keySet().containsAll(fields)) {
			throw new CelestaException("Not all of specified columns are existed!!!");
		}
		this.fields = fields;
		prepareOrderBy();
		fillFieldsForStatement();
	}

	PreparedStmtHolder getHereHolder() {
		// To be overriden in Cursor class
		return new OrderFieldsMaskedStatementHolder() {

			@Override
			protected PreparedStatement initStatement(List<ParameterSetter> program)
					throws CelestaException {
				WhereTerm where = qmaker.getWhereTerm('=');
				where.programParams(program);
				return db.getNavigationStatement(
						conn, getFrom(),"", where.getWhere(), fieldsForStatement, 0
				);
			}

		};
	}

	final void close(PreparedStmtHolder... stmts) {
		closed = true;
		closeStatements(stmts);
	}

	private void closeStatements(PreparedStmtHolder... stmts) {
		for (PreparedStmtHolder stmt : stmts) {
			stmt.close();
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
			context.removeFromCache(this);
			context.decCursorCount();
			close(set, forwards, backwards, here, first, last, count, position);
		}
	}

	final Map<String, AbstractFilter> getFilters() {
		return filters;
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
		PermissionManager permissionManager = callContext().getPermissionManager();
		return permissionManager.isActionAllowed(context, meta(), Action.READ);
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
		PermissionManager permissionManager = callContext().getPermissionManager();
		return permissionManager.isActionAllowed(context, meta(), Action.INSERT);
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
		PermissionManager permissionManager = callContext().getPermissionManager();
		return permissionManager.isActionAllowed(context, meta(), Action.MODIFY);
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
		PermissionManager permissionManager = callContext().getPermissionManager();
		return permissionManager.isActionAllowed(context, meta(), Action.DELETE);
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

	protected void closeSet() throws CelestaException {
		cursor = null;
		set.close();
		forwards.close();
		backwards.close();
		first.close();
		last.close();
		count.close();
		position.close();
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

	List<String> getOrderByFields() throws CelestaException {
		if (orderByNames == null)
			orderBy();
		return Arrays.asList(orderByNames);
	}

	String getReversedOrderBy() throws CelestaException {
		return getOrderBy(true);
	}

	/**
	 * Returns column names that are in sorting.
	 * 
	 * @throws CelestaException
	 *             cannot normally occur.
	 */
	public String[] orderByColumnNames() throws CelestaException {
		if (orderByNames == null)
			orderBy();
		return orderByNames;
	}

	/**
	 * Returns mask of DESC orders.
	 * 
	 * @throws CelestaException
	 *             cannot normally occur.
	 */
	public boolean[] descOrders() throws CelestaException {
		if (orderByNames == null)
			orderBy();
		return descOrders;
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

		PreparedStatement ps = set.getStatement(_currentValues(), 0);
		boolean result = false;
		try {
			if (cursor != null)
				cursor.close();
			cursor = ps.executeQuery();
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
	 *            <li>=обновить текущую запись (если она имеется в
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
					"Invalid navigation command: '%s', should consist of '+', '-', '>', '<' and '=' only!",
					command);


		if (navigationOffset != 0)
			closeStatements(backwards, forwards);

		navigationOffset = 0;
		for (int i = 0; i < command.length(); i++) {
			char c = command.charAt(i);
			PreparedStatement navigator = chooseNavigator(c);

			if (executeNavigator(navigator))
				return true;
		}
		return false;
	}

	public boolean navigate(String command, long offset) throws CelestaException {
		if (!canRead())
			throw new PermissionDeniedException(callContext(), meta(), Action.READ);

		Matcher m = NAVIGATION_WITH_OFFSET.matcher(command);
		if (!m.matches())
			throw new CelestaException(
					"Invalid navigation command: '%s', should consist only one of  '>' or '<'!",
					command);

		if (offset < 0)
			throw new CelestaException("Invalid navigation offset: offset should not be less than 0");

		if (navigationOffset != offset) {
			navigationOffset = offset;
			closeStatements(backwards, forwards);
		}

		PreparedStatement navigator = chooseNavigator(command.charAt(0));
		// System.out.println(navigator);
		return executeNavigator(navigator);

	}

	private boolean executeNavigator(PreparedStatement navigator) throws CelestaException {
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
			throw new CelestaException(NAVIGATING_ERROR, e.getMessage());
		}
		return false;
	}

	private PreparedStatement chooseNavigator(char c) throws CelestaException {
		Object[] rec = _currentValues();

		switch (c) {
		case '<':
			return backwards.getStatement(rec, 0);
		case '>':
			return forwards.getStatement(rec, 0);
		case '=':
			return here.getStatement(rec, 0);
		case '-':
			return first.getStatement(rec, 0);
		case '+':
			return last.getStatement(rec, 0);
		default:
			// THIS WILL NEVER EVER HAPPEN, WE'VE ALREADY CHECKED
			return null;
		}

	}

	WhereTermsMaker getQmaker() {
		return qmaker;
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
			if (closed)
				return;
			AbstractFilter oldFilter = filters.get(name);
			// Если один SingleValue меняется на другой SingleValue -- то
			// необязательно закрывать набор, можно использовать старый.
			if (oldFilter instanceof SingleValue) {
				((SingleValue) oldFilter).setValue(value);
			} else {
				filters.put(name, new SingleValue(value));
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
	public final void setRange(String name, Object valueFrom, Object valueTo)
			throws CelestaException {
		validateColumName(name);
		if (closed)
			return;
		AbstractFilter oldFilter = filters.get(name);
		// Если один Range меняется на другой Range -- то
		// необязательно закрывать набор, можно использовать старый.
		if (oldFilter instanceof Range) {
			((Range) oldFilter).setValues(valueFrom, valueTo);
		} else {
			filters.put(name, new Range(valueFrom, valueTo));
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
			throw new CelestaException(
					"Filter for column %s is null or empty. "
							+ "Use setrange(fieldname) to remove any filters from the column.",
					name);
		AbstractFilter oldFilter =
			filters.put(name, new Filter(value, meta().getColumns().get(name)));
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
		resetSpecificState();
		complexFilter = null;
		orderByNames = null;
		orderByIndices = null;
		descOrders = null;
		offset = 0;
		rowCount = 0;
		closeSet();
	}

	protected void resetSpecificState() {}

	/**
	 * Установка сортировки.
	 * 
	 * @param names
	 *            Перечень полей для сортировки.
	 * @throws CelestaException
	 *             неверное имя поля или SQL-ошибка.
	 */
	public final void orderBy(String... names) throws CelestaException {
		prepareOrderBy(names);

		if (!fieldsForStatement.isEmpty()) {
			fillFieldsForStatement();
		}

		closeSet();
	}

	private void prepareOrderBy(String... names) throws CelestaException {

		ArrayList<String> l = new ArrayList<>(8);
		ArrayList<Boolean> ol = new ArrayList<>(8);
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
				throw new CelestaException("Column '%s' is used more than once in orderby() call",
						colName);

			boolean order = !(m.group(2) == null || "asc".equalsIgnoreCase(m.group(2).trim()));

			l.add(String.format("\"%s\"", colName));

			ol.add(order);
		}

		appendPK(l, ol, colNames);

		orderByNames = new String[l.size()];
		orderByIndices = new int[l.size()];
		descOrders = new boolean[l.size()];
		for (int i = 0; i < orderByNames.length; i++) {
			orderByNames[i] = l.get(i);
			descOrders[i] = ol.get(i);
			orderByIndices[i] = meta().getColumnIndex(WhereTermsMaker.unquot(orderByNames[i]));
		}
	}

	abstract void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames)
			throws CelestaException;

	/**
	 * Сброс фильтров, сортировки и полная очистка буфера.
	 * 
	 * @throws CelestaException
	 *             SQL-ошибка.
	 */
	public void clear() throws CelestaException {
		_clearBuffer(true);
		filters.clear();
		clearSpecificState();
		complexFilter = null;

		if (!fieldsForStatement.isEmpty()) {
			prepareOrderBy();
			fillFieldsForStatement();
		}

		orderByNames = null;
		orderByIndices = null;
		descOrders = null;
		offset = 0;
		rowCount = 0;
		closeSet();
	}

	protected void clearSpecificState() {}

	/**
	 * Возвращает число записей в отфильтрованном наборе.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки доступа или ошибки БД
	 */
	public final int count() throws CelestaException {
		PreparedStatement stmt = count.getStatement(_currentValues(), 0);
		int result = count(stmt);
		// we are not holding this query: it's rarely used.
		count.close();
		return result;
	}

	/**
	 * A hidden method that returns total count of rows that precede the current
	 * one in the set. This method is intended for internal use by GridDriver.
	 * Since rows counting is a resource-consuming operation, this method should
	 * not be public.
	 */
	final int position() throws CelestaException {
		PreparedStatement stmt = position.getStatement(_currentValues(), 0);
		// System.out.println(stmt);
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
			throw new CelestaException(
					"Cannot assign filters from cursor for %s.%s to cursor for %s.%s.",
					c._grainName(), c._tableName(), _grainName(), _tableName());
		filters.clear();
		filters.putAll(c.filters);
		complexFilter = c.complexFilter;
		copySpecificFiltersFrom(c);
		offset = c.offset;
		rowCount = c.rowCount;
		closeSet();
	}

	protected void copySpecificFiltersFrom(BasicCursor c) {}

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
			throw new CelestaException(
					"Cannot assign ordering from cursor for %s.%s to cursor for %s.%s.",
					c._grainName(), c._tableName(), _grainName(), _tableName());
		orderByNames = c.orderByNames;
		orderByIndices = c.orderByIndices;
		descOrders = c.descOrders;
		closeSet();
	}

	boolean isEquivalent(BasicCursor c) throws CelestaException {
		// equality of all simple filters
		if (filters.size() != c.filters.size())
			return false;
		for (Map.Entry<String, AbstractFilter> e : filters.entrySet()) {
			if (!e.getValue().filterEquals(c.filters.get(e.getKey())))
				return false;
		}
		// equality of complex filter
		if (!(complexFilter == null ? c.complexFilter == null
				: complexFilter.getCSQL().equals(c.complexFilter.getCSQL())))
			return false;
		// equality of In filter
		if (!isEquivalentSpecific(c)) {
			return false;
		}
		// equality of sorting
		if (orderByNames == null)
			orderBy();
		if (c.orderByNames == null)
			c.orderBy();
		if (orderByNames.length != c.orderByNames.length)
			return false;
		for (int i = 0; i < orderByNames.length; i++) {
			if (!(orderByNames[i].equals(c.orderByNames[i])))
				return false;
			if (descOrders[i] != c.descOrders[i])
				return false;
		}
		return true;
	}


	boolean isEquivalentSpecific(BasicCursor c) throws CelestaException {
		return true;
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

	protected boolean inRec(String field) {
		return fieldsForStatement.isEmpty() || fieldsForStatement.contains(field);
	}

	protected In getIn() {
		return null;
	}

	protected FromClause getFrom() throws CelestaException {
		FromClause result = new FromClause();
		GrainElement ge = meta();

		result.setGe(ge);
		result.setExpression(String.format(db.tableTemplate(), ge.getGrain().getName(), ge.getName()));

		return result;
	}

	private void fillFieldsForStatement() throws CelestaException {
		fieldsForStatement.clear();
		fieldsForStatement = new HashSet<>(
				Arrays.asList(orderByColumnNames()).stream()
						.map(f -> f.replaceAll("\"",""))
						.collect(Collectors.toSet())
		);
		fieldsForStatement.addAll(fields);
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
	public abstract BasicCursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException;

	public abstract Object[] _currentValues();

	protected abstract void _clearBuffer(boolean withKeys);

	protected abstract String _grainName();

	protected abstract String _tableName();

	protected abstract void _parseResult(ResultSet rs) throws SQLException;

	protected abstract void _setFieldValue(String name, Object value);

	// CHECKSTYLE:ON

}
