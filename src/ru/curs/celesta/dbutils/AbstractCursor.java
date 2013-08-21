package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.score.Table;

/**
 * Базовый класс курсора (аналог соответствующего класса в Python-коде).
 */
abstract class AbstractCursor {
	private final DBAdaptor db;
	private final Connection conn;
	private PreparedStatement get = null;
	private PreparedStatement set = null;
	private PreparedStatement insert = null;
	private PreparedStatement update = null;
	private PreparedStatement delete = null;

	private ResultSet cursor = null;

	private Map<String, AbstractFilter> filters = new HashMap<>();
	private List<String> orderBy = new LinkedList<>();

	public AbstractCursor(Connection conn) throws CelestaCritical {
		try {
			if (conn.isClosed())
				throw new CelestaCritical(
						"Trying to create a cursor on closed connection.");
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
		this.conn = conn;
		db = DBAdaptor.getAdaptor();
	}

	@Override
	protected void finalize() throws Throwable {
		if (get != null)
			get.close();
		if (set != null)
			set.close();
	}

	private void validateColumName(String name) throws CelestaCritical {
		if (!meta().getColumns().containsKey(name))
			throw new CelestaCritical("No column %s exists in table %s.", name,
					tableName());
	}

	private void closeSet() throws CelestaCritical {
		cursor = null;
		if (set != null) {
			try {
				set.close();
			} catch (SQLException e) {
				throw new CelestaCritical(e.getMessage());
			}
			set = null;
		}
	}

	/**
	 * Переходит к первой записи в отфильтрованном наборе и возвращает
	 * информацию об успешности перехода.
	 * 
	 * @return true, если переход успешен, false -- если записей в наборе нет.
	 */
	public final boolean tryFirst() throws CelestaCritical {
		if (set == null)
			set = db.getRecordSetStatement(conn, meta(), filters, orderBy);
		boolean result = false;
		try {
			if (cursor != null)
				cursor.close();
			cursor = set.executeQuery();
			result = cursor.next();
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
		if (result)
			parseResult(cursor);
		return result;
	}

	/**
	 * Переходит к первой записи в отфильтрованном наборе, вызывая ошибку в
	 * случае, если переход неудачен.
	 * 
	 * @throws CelestaCritical
	 *             в случае, если записей в наборе нет.
	 */
	public final void first() throws CelestaCritical {
		if (!tryFirst()) {
			StringBuilder sb = new StringBuilder();
			for (Entry<String, AbstractFilter> e : filters.entrySet()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(String.format("%s=%s", e.getKey(), e.getValue()
						.toString()));
				throw new CelestaCritical("There is no %s (%s).", tableName(),
						sb.toString());
			}
		}
	}

	/**
	 * Переходит к следующей записи в отсортированном наборе. Возвращает false,
	 * если достигнут конец набора.
	 * 
	 * @throws CelestaCritical
	 *             в случае ошибки БД
	 */
	public final boolean next() throws CelestaCritical {
		boolean result = false;
		if (cursor == null)
			result = tryFirst();
		else {
			try {
				result = cursor.next();
			} catch (SQLException e) {
				result = false;
			}
		}
		if (result)
			parseResult(cursor);
		return result;
	}

	/**
	 * Осуществляет вставку курсора в БД.
	 */
	public final void insert() throws CelestaCritical {
		if (!tryInsert()) {
			StringBuilder sb = new StringBuilder();
			for (Object value : currentKeyValues()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaCritical("Record %s (%s) already exists",
					tableName(), sb.toString());
		}
	}

	/**
	 * Осуществляет вставку курсора в БД.
	 * 
	 * @throws CelestaCritical
	 *             ошибка БД
	 */
	public final boolean tryInsert() throws CelestaCritical {
		prepareGet(currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (rs.next())
					return false;
			} finally {
				rs.close();
			}
			if (insert == null)
				insert = db.getInsertRecordStatement(conn, meta());
			Object[] values = currentValues();
			for (int i = 0; i < values.length; i++)
				DBAdaptor.setParam(insert, i + 1, values[i]);
			insert.execute();

		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
		return true;

	}

	/**
	 * Осуществляет сохранение содержимого курсора в БД, выбрасывая исключение в
	 * случае, если запись с такими ключевыми полями не найдена.
	 */
	public final void update() throws CelestaCritical {
		if (!tryUpdate()) {
			StringBuilder sb = new StringBuilder();
			for (Object value : currentKeyValues()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaCritical("Record %s (%s) does not exist.",
					tableName(), sb.toString());
		}
	}

	/**
	 * Осуществляет сохранение содержимого курсора в БД.
	 * 
	 * @throws CelestaCritical
	 *             ошибка БД
	 */
	public final boolean tryUpdate() throws CelestaCritical {
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
			for (int i = 0; i < values.length; i++)
				DBAdaptor.setParam(update, i + 1, values[i]);
			for (int i = 0; i < keyValues.length; i++)
				DBAdaptor.setParam(update, i + values.length + 1, values[i]);
			update.execute();

		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
		return true;
	}

	/**
	 * Удаляет текущую запись.
	 * 
	 * @throws CelestaCritical
	 *             ошибка БД
	 */
	public void delete() throws CelestaCritical {
		if (delete == null)
			delete = db.getDeleteRecordStatement(conn, meta());
		Object[] keyValues = currentKeyValues();
		for (int i = 0; i < keyValues.length; i++)
			DBAdaptor.setParam(delete, i + 1, keyValues[i]);
		try {
			delete.execute();
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
	}

	/**
	 * Осуществляет поиск записи по ключевым полям, выбрасывает исключение, если
	 * запись не найдена.
	 * 
	 * @param values
	 *            значения ключевых полей
	 * @throws CelestaCritical
	 *             в случае, если запись не найдена
	 */
	public final void get(Object... values) throws CelestaCritical {
		if (!tryGet(values)) {
			StringBuilder sb = new StringBuilder();
			for (Object value : values) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(value == null ? "null" : value.toString());
			}
			throw new CelestaCritical("There is no %s (%s).", tableName(),
					sb.toString());
		}
	}

	/**
	 * Осуществляет поиск записи по ключевым полям, возвращает значение --
	 * найдена запись или нет.
	 * 
	 * @param values
	 *            значения ключевых полей
	 * @throws CelestaCritical
	 *             SQL-ошибка
	 */

	public final boolean tryGet(Object... values) throws CelestaCritical {
		prepareGet(values);
		boolean result = false;
		try {
			ResultSet rs = get.executeQuery();
			try {
				result = rs.next();
			} finally {
				rs.close();
			}
			if (result)
				parseResult(rs);
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
		return result;
	}

	private void prepareGet(Object... values) throws CelestaCritical {
		if (get == null)
			get = db.getOneRecordStatement(conn, meta());
		if (meta().getPrimaryKey().size() != values.length)
			throw new CelestaCritical(
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
	 * @throws CelestaCritical
	 *             Неверное имя поля.
	 */
	public final void setRange(String name) throws CelestaCritical {
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
	 * @throws CelestaCritical
	 *             Неверное имя поля
	 */
	public final void setRange(String name, Object value)
			throws CelestaCritical {
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
	 * @throws CelestaCritical
	 *             Неверное имя поля, SQL-ошибка.
	 */
	public final void setRange(String name, Object valueFrom, Object valueTo)
			throws CelestaCritical {
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
	 * @throws CelestaCritical
	 *             Неверное имя поля и т. п.
	 */
	public final void setFilter(String name, String value)
			throws CelestaCritical {
		validateColumName(name);
		filters.put(name, new Filter(value));
		closeSet();
	}

	/**
	 * Установка сортировки.
	 * 
	 * @param names
	 *            Перечень полей для сортировки.
	 * @throws CelestaCritical
	 *             неверное имя поля или SQL-ошибка.
	 */
	public final void orderBy(String... names) throws CelestaCritical {
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
	 * @throws CelestaCritical
	 *             SQL-ошибка.
	 */
	public final void reset() throws CelestaCritical {
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
	 * @throws CelestaCritical
	 *             SQL-ошибка.
	 */
	public final void clear() throws CelestaCritical {
		clearBuffer(true);
		filters.clear();
		orderBy.clear();
		closeSet();
	}

	/**
	 * Имя таблицы.
	 * 
	 * @throws CelestaCritical
	 *             ошибка метаданных
	 */
	public final String tableName() throws CelestaCritical {
		return meta().getName();
	}

	/**
	 * Описание таблицы (метаинформация).
	 */
	public abstract Table meta() throws CelestaCritical;

	abstract void parseResult(ResultSet rs);

	abstract void clearBuffer(boolean withKeys);

	abstract Object[] currentKeyValues();

	abstract Object[] currentValues();
}
