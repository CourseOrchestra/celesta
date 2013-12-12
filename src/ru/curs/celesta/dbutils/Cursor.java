package ru.curs.celesta.dbutils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/**
 * Базовый класс курсора (аналог соответствующего класса в Python-коде).
 */
public abstract class Cursor {
	static final String SYSTEMUSERID = String.format("SYS%08X",
			(new Random()).nextInt());

	private static final PermissionManager PERMISSION_MGR = new PermissionManager();
	private static final LoggingManager LOGGING_MGR = new LoggingManager();
	private static final Pattern COLUMN_NAME = Pattern
			.compile("([a-zA-Z_][a-zA-Z0-9_]*)"
					+ "( +([Aa]|[Dd][Ee])[Ss][Cc])?");

	private Table meta = null;
	private final DBAdaptor db;
	private final Connection conn;
	private final CallContext context;
	private PreparedStatement get = null;
	private PreparedStatement set = null;
	private boolean[] insertMask = null;
	private boolean[] updateMask = null;
	private PreparedStatement insert = null;
	private PreparedStatement update = null;
	private PreparedStatement delete = null;

	private ResultSet cursor = null;
	private Cursor xRec;

	// Поля фильтров и сортировок
	private Map<String, AbstractFilter> filters = new HashMap<>();
	private String orderBy = null;
	private Range<Long> limit = new Range<Long>(0L, 0L);

	public Cursor(CallContext context) throws CelestaException {
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
		if (get != null)
			get.close();
		if (set != null)
			set.close();
	}

	private void validateColumName(String name) throws CelestaException {
		if (!meta().getColumns().containsKey(name))
			throw new CelestaException("No column %s exists in table %s.",
					name, _tableName());
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

	/**
	 * Возвращает контекст вызова, в котором создан данный курсор.
	 */
	public final CallContext callContext() {
		return context;
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
		if (!PERMISSION_MGR.isActionAllowed(context, meta(), Action.READ))
			throw new PermissionDeniedException(context, meta(), Action.READ);

		if (set == null)
			set = db.getRecordSetStatement(conn, meta(), filters, getOrderBy(),
					limit);
		boolean result = false;
		try {
			if (cursor != null)
				cursor.close();
			cursor = set.executeQuery();
			result = cursor.next();
			if (result) {
				_parseResult(cursor);
				xRec = _getBufferCopy();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
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
				throw new CelestaException("There is no %s (%s).",
						_tableName(), sb.toString());
			}
		}
	}

	/**
	 * Возвращает число записей в отфильтрованном наборе.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки доступа или ошибки БД
	 */
	public final int count() throws CelestaException {
		int result;
		PreparedStatement stmt = db.getSetCountStatement(conn, meta(), filters);
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
				xRec = _getBufferCopy();
			}
		} catch (SQLException e) {
			result = false;
		}
		return result;
	}

	/**
	 * Осуществляет вставку курсора в БД.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки БД
	 */
	public final void insert() throws CelestaException {
		if (!tryInsert()) {
			StringBuilder sb = new StringBuilder();
			for (Object value : _currentKeyValues()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaException("Record %s (%s) already exists",
					_tableName(), sb.toString());
		}
	}

	/**
	 * Осуществляет вставку курсора в БД.
	 * 
	 * @throws CelestaException
	 *             ошибка БД
	 */
	public final boolean tryInsert() throws CelestaException {
		if (!PERMISSION_MGR.isActionAllowed(context, meta(), Action.INSERT))
			throw new PermissionDeniedException(context, meta(), Action.INSERT);

		prepareGet(_currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (rs.next())
					return false;
			} finally {
				rs.close();
			}
			Object[] values = _currentValues();
			boolean[] myMask = new boolean[values.length];
			for (int i = 0; i < values.length; i++)
				myMask[i] = values[i] == null;
			if (!Arrays.equals(myMask, insertMask)) {
				insert = db.getInsertRecordStatement(conn, meta(), myMask);
				insertMask = myMask;
			}

			int j = 1;
			for (int i = 0; i < values.length; i++)
				if (!myMask[i]) {
					DBAdaptor.setParam(insert, j, values[i]);
					j++;
				}
			_preInsert();
			insert.execute();
			LOGGING_MGR.log(this, Action.INSERT);
			for (Column c : meta().getColumns().values())
				if (c instanceof IntegerColumn
						&& ((IntegerColumn) c).isIdentity()) {
					_setAutoIncrement(db.getCurrentIdent(conn, meta()));
					break;
				}
			internalGet(_currentKeyValues());
			xRec = _getBufferCopy();
			_postInsert();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return true;
	}

	/**
	 * Осуществляет сохранение содержимого курсора в БД, выбрасывая исключение в
	 * случае, если запись с такими ключевыми полями не найдена.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки БД
	 */
	public final void update() throws CelestaException {
		if (!tryUpdate()) {
			StringBuilder sb = new StringBuilder();
			for (Object value : _currentKeyValues()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaException("Record %s (%s) does not exist.",
					_tableName(), sb.toString());
		}
	}

	/**
	 * Осуществляет сохранение содержимого курсора в БД.
	 * 
	 * @throws CelestaException
	 *             ошибка БД
	 */
	public final boolean tryUpdate() throws CelestaException {
		if (!PERMISSION_MGR.isActionAllowed(context, meta(), Action.MODIFY))
			throw new PermissionDeniedException(context, meta(), Action.MODIFY);

		prepareGet(_currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (!rs.next())
					return false;
			} finally {
				rs.close();
			}
			Object[] values = _currentValues();
			Object[] xValues = getXRec()._currentValues();
			// Маска: true для тех случаев, когда поле не было изменено
			boolean[] myMask = new boolean[values.length];
			boolean notChanged = true;
			for (int i = 0; i < values.length; i++) {
				myMask[i] = compareValues(values[i], xValues[i]);
				notChanged &= myMask[i];
			}
			// Если ничего не изменилось -- выполнять дальнейшие действия нет
			// необходимости
			if (notChanged)
				return true;
			if (!Arrays.equals(myMask, updateMask)) {
				update = db.getUpdateRecordStatement(conn, meta(), myMask);
				updateMask = myMask;
			}

			Object[] keyValues = _currentKeyValues();

			// Заполняем параметры присвоения (set ...)
			int j = 1;
			int i = 0;
			for (String c : meta().getColumns().keySet()) {
				if (!(myMask[i] || meta().getPrimaryKey().containsKey(c))) {
					DBAdaptor.setParam(update, j, values[i]);
					j++;
				}
				i++;
			}
			// Заполняем параметры поиска (where ...)
			for (i = 0; i < keyValues.length; i++)
				DBAdaptor.setParam(update, i + j, keyValues[i]);

			_preUpdate();
			update.execute();
			LOGGING_MGR.log(this, Action.MODIFY);
			xRec = _getBufferCopy();
			_postUpdate();

		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return true;
	}

	/**
	 * Сравнивает значения для того, чтобы определить: что именно было изменено
	 * в записи. Возвращает true, если значения изменены не были.
	 * 
	 * @param newVal
	 *            новое значение
	 * @param oldVal
	 *            старое значение
	 */
	private static boolean compareValues(Object newVal, Object oldVal) {
		if (newVal == null)
			return oldVal == null || (oldVal instanceof BLOB);
		if (newVal instanceof BLOB)
			return !((BLOB) newVal).isModified();
		return newVal.equals(oldVal);
	}

	/**
	 * Удаляет текущую запись.
	 * 
	 * @throws CelestaException
	 *             ошибка БД
	 */
	public final void delete() throws CelestaException {
		if (!PERMISSION_MGR.isActionAllowed(context, meta(), Action.DELETE))
			throw new PermissionDeniedException(context, meta(), Action.DELETE);

		if (delete == null)
			delete = db.getDeleteRecordStatement(conn, meta());
		Object[] keyValues = _currentKeyValues();
		for (int i = 0; i < keyValues.length; i++)
			DBAdaptor.setParam(delete, i + 1, keyValues[i]);
		try {
			_preDelete();
			delete.execute();
			LOGGING_MGR.log(this, Action.DELETE);
			xRec = _getBufferCopy();
			_postDelete();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	/**
	 * Удаляет все записи, попавшие в текущий фильтр.
	 * 
	 * @throws CelestaException
	 *             Ошибка БД
	 */
	public final void deleteAll() throws CelestaException {
		if (!PERMISSION_MGR.isActionAllowed(context, meta(), Action.DELETE))
			throw new PermissionDeniedException(context, meta(), Action.DELETE);

		PreparedStatement stmt = db.deleteRecordSetStatement(conn, meta(),
				filters);
		try {
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	/**
	 * Осуществляет поиск записи по ключевым полям, выбрасывает исключение, если
	 * запись не найдена.
	 * 
	 * @param values
	 *            значения ключевых полей
	 * @throws CelestaException
	 *             в случае, если запись не найдена
	 */
	public final void get(Object... values) throws CelestaException {
		if (!tryGet(values)) {
			StringBuilder sb = new StringBuilder();
			for (Object value : values) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaException("There is no %s (%s).", _tableName(),
					sb.toString());
		}
	}

	/**
	 * Осуществляет поиск записи по ключевым полям, возвращает значение --
	 * найдена запись или нет.
	 * 
	 * @param values
	 *            значения ключевых полей
	 * @throws CelestaException
	 *             SQL-ошибка
	 */

	public final boolean tryGet(Object... values) throws CelestaException {
		if (!PERMISSION_MGR.isActionAllowed(context, meta(), Action.READ))
			throw new PermissionDeniedException(context, meta(), Action.READ);
		return internalGet(values);
	}

	private boolean internalGet(Object... values) throws CelestaException {
		prepareGet(values);
		boolean result = false;
		try {
			ResultSet rs = get.executeQuery();
			try {
				result = rs.next();
				if (result) {
					_parseResult(rs);
					xRec = _getBufferCopy();
				}
			} finally {
				rs.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	private void prepareGet(Object... values) throws CelestaException {
		if (get == null)
			get = db.getOneRecordStatement(conn, meta());
		if (meta().getPrimaryKey().size() != values.length)
			throw new CelestaException(
					"Invalid number of 'get' arguments for '%s': expected %d, provided %d.",
					_tableName(), meta().getPrimaryKey().size(), values.length);

		for (int i = 0; i < values.length; i++)
			DBAdaptor.setParam(get, i + 1, values[i]);

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
	 * Прочитывает содержимое BLOB-поля в память.
	 * 
	 * @param name
	 *            имя поля
	 * @throws CelestaException
	 *             Неверное имя поля
	 */
	protected BLOB calcBlob(String name) throws CelestaException {
		validateColumName(name);
		Column c = meta().getColumns().get(name);
		if (!(c instanceof BinaryColumn))
			throw new CelestaException("'%s' is not a BLOB column.",
					c.getName());
		BLOB result;
		PreparedStatement stmt = db.getOneFieldStatement(conn, c);
		Object[] keyVals = _currentKeyValues();
		for (int i = 0; i < keyVals.length; i++)
			DBAdaptor.setParam(stmt, i + 1, keyVals[i]);
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				if (rs.next()) {
					Blob b = rs.getBlob(1);
					if (!(b == null || rs.wasNull()))
						try {
							InputStream is = b.getBinaryStream();
							try {
								result = new BLOB(is);
							} finally {
								is.close();
							}
						} finally {
							b.free();
						}
					else {
						// Поле имеет значение null
						result = new BLOB();
					}
				} else {
					// Записи не существует вовсе
					result = new BLOB();
				}
			} finally {
				rs.close();
			}
			stmt.close();
		} catch (SQLException | IOException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	/**
	 * Возвращает максимальную длину текстового поля (если она определена).
	 * 
	 * @param name
	 *            Имя текстового поля.
	 * @return длина текстового поля или -1 (минус единица) если вместо длины
	 *         указано MAX.
	 * @throws CelestaException
	 *             Если указано имя несуществующего или нетекстового поля.
	 */
	public final int getMaxStrLen(String name) throws CelestaException {
		validateColumName(name);
		Column c = meta().getColumns().get(name);
		if (c instanceof StringColumn) {
			StringColumn sc = (StringColumn) c;
			return sc.isMax() ? -1 : sc.getLength();
		} else {
			throw new CelestaException("Column %s is not of string type.",
					c.getName());
		}
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
		filters.put(name, new Range<Object>(valueFrom, valueTo));
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
		filters.put(name, new Filter(value));
		closeSet();
	}

	/**
	 * Устанавливает фильтр на диапазон возвращаемых курсором записей.
	 * 
	 * @param skip
	 *            Количество записей, которое необходимо пропустить (0 -
	 *            начинать с начала).
	 * @param count
	 *            Максимальное количество записей, которое необходимо вернуть (0
	 *            - вернуть все записи).
	 * @throws CelestaException
	 *             ошибка БД.
	 */
	public final void limit(long skip, long count) throws CelestaException {
		limit = new Range<Long>(skip, count);
		closeSet();
	}

	private String getOrderBy() throws CelestaException {
		if (orderBy == null)
			orderBy();
		return orderBy;
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
		// Всегда добавляем в конец OrderBy поля первичного ключа, идующие в
		// естественном порядке
		for (String colName : meta().getPrimaryKey().keySet())
			if (!colNames.contains(colName)) {
				if (needComma)
					orderByClause.append(", ");
				orderByClause.append(String.format("\"%s\"", colName));
				needComma = true;
			}

		orderBy = orderByClause.toString();
	}

	/**
	 * Сброс фильтров и сортировки.
	 * 
	 * @throws CelestaException
	 *             SQL-ошибка.
	 */
	public final void reset() throws CelestaException {
		filters.clear();
		orderBy = null;
		closeSet();
	}

	/**
	 * Очистка всех полей буфера, кроме ключевых.
	 */
	public final void init() {
		_clearBuffer(false);
	}

	/**
	 * Сброс фильтров, сортировки и полная очистка буфера.
	 * 
	 * @throws CelestaException
	 *             SQL-ошибка.
	 */
	public final void clear() throws CelestaException {
		_clearBuffer(true);
		filters.clear();
		orderBy = null;
		closeSet();
	}

	/**
	 * Описание таблицы (метаинформация).
	 * 
	 * @throws CelestaException
	 *             в случае ошибки извлечения метаинформации (в норме не должна
	 *             происходить).
	 */
	public final Table meta() throws CelestaException {
		if (meta == null)
			try {
				meta = Celesta.getInstance().getScore().getGrain(_grainName())
						.getTable(_tableName());
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
		return meta;
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

	/**
	 * Возвращает копию буфера, содержащую значения, полученные при последнем
	 * чтении данных из базы.
	 */
	public final Cursor getXRec() {
		if (xRec == null) {
			try {
				xRec = _getBufferCopy();
				xRec.clear();
			} catch (CelestaException e) {
				xRec = null;
			}
		}
		return xRec;
	}

	/**
	 * Копировать значения полей из курсора того же типа.
	 * 
	 * @param from
	 *            курсор, из которого следует скопировать значения полей
	 */
	public abstract void copyFieldsFrom(Cursor from);

	// CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */

	protected abstract Cursor _getBufferCopy() throws CelestaException;

	protected abstract String _grainName();

	protected abstract String _tableName();

	protected abstract void _parseResult(ResultSet rs) throws SQLException;

	protected abstract void _clearBuffer(boolean withKeys);

	protected abstract Object[] _currentKeyValues();

	protected abstract Object[] _currentValues();

	protected abstract void _setAutoIncrement(int val);

	protected abstract void _preDelete();

	protected abstract void _postDelete();

	protected abstract void _preUpdate();

	protected abstract void _postUpdate();

	protected abstract void _preInsert();

	protected abstract void _postInsert();

	// CHECKSTYLE:ON
}
