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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.IntegerColumn;
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

	private Table meta = null;
	private final DBAdaptor db;
	private final Connection conn;
	private final CallContext context;
	private PreparedStatement get = null;
	private PreparedStatement set = null;
	private boolean[] insertMask = null;
	private PreparedStatement insert = null;
	private PreparedStatement update = null;
	private PreparedStatement delete = null;

	private ResultSet cursor = null;
	private Cursor xRec;

	private Map<String, AbstractFilter> filters = new HashMap<>();
	private List<String> orderBy = new LinkedList<>();

	public Cursor(CallContext context) throws CelestaException {
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
					name, tableName());
	}

	private void closeSet() throws CelestaException {
		cursor = null;
		if (set != null) {
			try {
				set.close();
			} catch (SQLException e) {
				throw new CelestaException(e.getMessage());
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
			set = db.getRecordSetStatement(conn, meta(), filters, orderBy);
		boolean result = false;
		try {
			if (cursor != null)
				cursor.close();
			cursor = set.executeQuery();
			result = cursor.next();
			if (result) {
				parseResult(cursor);
				xRec = getBufferCopy();
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
				throw new CelestaException("There is no %s (%s).", tableName(),
						sb.toString());
			}
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
				parseResult(cursor);
				xRec = getBufferCopy();
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
			for (Object value : currentKeyValues()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaException("Record %s (%s) already exists",
					tableName(), sb.toString());
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

		prepareGet(currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (rs.next())
					return false;
			} finally {
				rs.close();
			}
			Object[] values = currentValues();
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
			preInsert();
			insert.execute();
			LOGGING_MGR.log(this, Action.INSERT);
			for (Column c : meta().getColumns().values())
				if (c instanceof IntegerColumn
						&& ((IntegerColumn) c).isIdentity()) {
					setAutoIncrement(db.getCurrentIdent(conn, meta()));
					break;
				}
			internalGet(currentKeyValues());
			xRec = getBufferCopy();
			postInsert();
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
			for (Object value : currentKeyValues()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaException("Record %s (%s) does not exist.",
					tableName(), sb.toString());
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

		prepareGet(currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (!rs.next())
					return false;
			} finally {
				rs.close();
			}
			if (update == null)
				update = db.getUpdateRecordStatement(conn, meta());
			Object[] values = currentValues();
			Object[] keyValues = currentKeyValues();

			// Заполняем параметры присвоения (set ...)
			int j = 1;
			int i = 0;
			for (String c : meta().getColumns().keySet()) {
				if (!meta().getPrimaryKey().containsKey(c)) {
					DBAdaptor.setParam(update, j, values[i]);
					j++;
				}
				i++;
			}
			// Заполняем параметры поиска (where ...)
			for (i = 0; i < keyValues.length; i++)
				DBAdaptor.setParam(update, i + j, values[i]);

			preUpdate();
			update.execute();
			LOGGING_MGR.log(this, Action.MODIFY);
			xRec = getBufferCopy();
			postUpdate();

		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return true;
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
		Object[] keyValues = currentKeyValues();
		for (int i = 0; i < keyValues.length; i++)
			DBAdaptor.setParam(delete, i + 1, keyValues[i]);
		try {
			preDelete();
			delete.execute();
			LOGGING_MGR.log(this, Action.DELETE);
			xRec = getBufferCopy();
			postDelete();
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
			throw new CelestaException("There is no %s (%s).", tableName(),
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
					parseResult(rs);
					xRec = getBufferCopy();
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
					tableName(), meta().getPrimaryKey().size(), values.length);

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
		Object[] keyVals = currentKeyValues();
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
		filters.put(name, new Filter(value));
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
		for (String name : names)
			validateColumName(name);
		orderBy.clear();
		for (String name : names)
			orderBy.add(name);
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
		orderBy.clear();
		closeSet();
	}

	/**
	 * Очистка всех полей буфера, кроме ключевых.
	 */
	public final void init() {
		clearBuffer(false);
	}

	/**
	 * Сброс фильтров, сортировки и полная очистка буфера.
	 * 
	 * @throws CelestaException
	 *             SQL-ошибка.
	 */
	public final void clear() throws CelestaException {
		clearBuffer(true);
		filters.clear();
		orderBy.clear();
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
				meta = Celesta.getInstance().getScore().getGrain(grainName())
						.getTable(tableName());
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
		Object[] values = currentValues();
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
				xRec = getBufferCopy();
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

	protected abstract Cursor getBufferCopy() throws CelestaException;

	protected abstract String grainName();

	protected abstract String tableName();

	protected abstract void parseResult(ResultSet rs) throws SQLException;

	protected abstract void clearBuffer(boolean withKeys);

	protected abstract Object[] currentKeyValues();

	protected abstract Object[] currentValues();

	protected abstract void setAutoIncrement(int val);

	protected abstract void preDelete();

	protected abstract void postDelete();

	protected abstract void preUpdate();

	protected abstract void postUpdate();

	protected abstract void preInsert();

	protected abstract void postInsert();
}
