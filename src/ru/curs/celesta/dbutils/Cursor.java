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

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;

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
 * Базовый класс курсора для модификации данных в таблицах.
 */
public abstract class Cursor extends BasicCursor {

	private static final LoggingManager LOGGING_MGR = new LoggingManager();

	private Table meta = null;
	private PreparedStatement get = null;
	private boolean[] insertMask = null;
	private boolean[] updateMask = null;
	private PreparedStatement insert = null;
	private PreparedStatement update = null;
	private PreparedStatement delete = null;

	private Cursor xRec;
	private int recversion;

	public Cursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	public final void close() {
		super.close();
		close(get, insert, delete, update);
	}

	@Override
	protected void finalize() throws Throwable {
		close(get, insert, delete, update);
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
		if (!canInsert())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.INSERT);

		prepareGet(_currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (rs.next()) {
					getXRec()._parseResult(rs);
					/*
					 * transmit recversion from xRec to rec for possible future
					 * record update
					 */
					if (getRecversion() == 0)
						setRecversion(xRec.getRecversion());
					return false;
				}
			} finally {
				rs.close();
			}
			_preInsert();
			Object[] values = _currentValues();
			boolean[] myMask = new boolean[values.length];
			for (int i = 0; i < values.length; i++)
				myMask[i] = values[i] == null;
			if (!Arrays.equals(myMask, insertMask)) {
				insert = db().getInsertRecordStatement(conn(), meta(), myMask);
				insertMask = myMask;
			}
			int j = 1;
			for (int i = 0; i < values.length; i++)
				if (!myMask[i]) {
					DBAdaptor.setParam(insert, j, values[i]);
					j++;
				}
			insert.execute();
			LOGGING_MGR.log(this, Action.INSERT);
			for (Column c : meta().getColumns().values())
				if (c instanceof IntegerColumn
						&& ((IntegerColumn) c).isIdentity()) {
					_setAutoIncrement(db().getCurrentIdent(conn(), meta()));
					break;
				}
			internalGet(_currentKeyValues());
			initXRec();
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
	public final void setValue(String name, Object value)
			throws CelestaException {
		validateColumName(name);
		_setFieldValue(name, value);
	}

	/**
	 * Осуществляет сохранение содержимого курсора в БД.
	 * 
	 * @throws CelestaException
	 *             ошибка БД
	 */
	// CHECKSTYLE:OFF for cyclomatic complexity
	public final boolean tryUpdate() throws CelestaException {
		// CHECKSTYLE:ON
		if (!canModify())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.MODIFY);

		prepareGet(_currentKeyValues());
		try {
			ResultSet rs = get.executeQuery();
			try {
				if (!rs.next())
					return false;
				// Прочитали из базы данных значения -- обновляем xRec
				if (xRec == null) {
					xRec = _getBufferCopy();
					// Вопрос на будущее: эта строчка должна быть здесь или за
					// фигурной скобкой? (проблема совместной работы над базой)
					xRec._parseResult(rs);
				}
			} finally {
				rs.close();
			}

			_preUpdate();
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
				update = db().getUpdateRecordStatement(conn(), meta(), myMask);
				updateMask = myMask;
			}

			Object[] keyValues = _currentKeyValues();
			int j = 1;
			int i = 0;
			// Для версионированной таблицы заполняем параметр с версией
			if (meta().isVersioned())
				DBAdaptor.setParam(update, j++, recversion);

			// Заполняем параметры присвоения (set ...)
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

			update.execute();
			LOGGING_MGR.log(this, Action.MODIFY);
			if (meta().isVersioned())
				recversion++;
			initXRec();
			_postUpdate();

		} catch (SQLException e) {
			if (e.getMessage().contains("record version check failure")) {
				throw new CelestaException(
						"Can not update %s.%s(%s): this record has been already modified"
								+ " by someone. Please start updating again.",
						meta().getGrain().getName(), meta().getName(),
						Arrays.toString(_currentKeyValues()));
			} else {
				throw new CelestaException("Update of %s.%s (%s) failure: %s",
						meta().getGrain().getName(), meta().getName(),
						Arrays.toString(_currentKeyValues()), e.getMessage());
			}
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
		if (!canDelete())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.DELETE);

		if (delete == null)
			delete = db().getDeleteRecordStatement(conn(), meta());
		Object[] keyValues = _currentKeyValues();
		for (int i = 0; i < keyValues.length; i++)
			DBAdaptor.setParam(delete, i + 1, keyValues[i]);
		try {
			_preDelete();
			delete.execute();
			LOGGING_MGR.log(this, Action.DELETE);
			initXRec();
			_postDelete();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	final void initXRec() throws CelestaException {
		if (xRec == null) {
			xRec = _getBufferCopy();
		} else {
			xRec.copyFieldsFrom(this);
		}
	}

	/**
	 * Удаляет все записи, попавшие в текущий фильтр.
	 * 
	 * @throws CelestaException
	 *             Ошибка БД
	 */
	public final void deleteAll() throws CelestaException {
		if (!canDelete())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.DELETE);

		PreparedStatement stmt = db().deleteRecordSetStatement(conn(), meta(),
				getFilters(), getComplexFilterExpr());
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
		if (!canRead())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.READ);
		return internalGet(values);
	}

	/**
	 * Получает из базы данных запись, соответствующую полям текущего первичного
	 * ключа.
	 * 
	 * @throws CelestaException
	 *             Ошибка доступа или взаимодействия с БД.
	 */
	public final boolean tryGetCurrent() throws CelestaException {
		if (!canRead())
			throw new PermissionDeniedException(callContext(), meta(),
					Action.READ);
		return internalGet(_currentKeyValues());
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
					initXRec();
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
			get = db().getOneRecordStatement(conn(), meta());
		if (meta().getPrimaryKey().size() != values.length)
			throw new CelestaException(
					"Invalid number of 'get' arguments for '%s': expected %d, provided %d.",
					_tableName(), meta().getPrimaryKey().size(), values.length);

		for (int i = 0; i < values.length; i++)
			DBAdaptor.setParam(get, i + 1, values[i]);

	}

	/**
	 * Устанавливает версию записи.
	 * 
	 * @param v
	 *            новая версия.
	 */
	public final void setRecversion(int v) {
		recversion = v;
	}

	/**
	 * Читает версию записи.
	 */
	public final int getRecversion() {
		return recversion;
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
		PreparedStatement stmt = db().getOneFieldStatement(conn(), c);
		Object[] keyVals = _currentKeyValues();
		for (int i = 0; i < keyVals.length; i++)
			DBAdaptor.setParam(stmt, i + 1, keyVals[i]);
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				if (rs.next()) {
					InputStream is = rs.getBinaryStream(1);
					if (!(is == null || rs.wasNull())) {
						try {
							result = new BLOB(is);
						} finally {
							is.close();
						}
					} else {
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
	 * Очистка всех полей буфера, кроме ключевых.
	 */
	public final void init() {
		_clearBuffer(false);
		xRec = null;
	}

	/**
	 * Описание таблицы (метаинформация).
	 * 
	 * @throws CelestaException
	 *             в случае ошибки извлечения метаинформации (в норме не должна
	 *             происходить).
	 */
	@Override
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

	@Override
	final void appendPK(StringBuilder orderByClause, boolean needComma,
			Set<String> colNames) throws CelestaException {
		boolean nc = needComma;
		// Всегда добавляем в конец OrderBy поля первичного ключа, идующие в
		// естественном порядке
		for (String colName : meta().getPrimaryKey().keySet())
			if (!colNames.contains(colName)) {
				if (nc)
					orderByClause.append(", ");
				orderByClause.append(String.format("\"%s\"", colName));
				nc = true;
			}
	}

	@Override
	String getNavigationWhereClause(char op) throws CelestaException {
		if (op == '=') {
			db();
			return "(" + DBAdaptor.getRecordWhereClause(meta()) + ")";
		} else {
			return super.getNavigationWhereClause(op);
		}
	}

	@Override
	public final void clear() throws CelestaException {
		super.clear();
		xRec = null;
	}

	/**
	 * Возвращает копию буфера, содержащую значения, полученные при последнем
	 * чтении данных из базы.
	 */
	public final Cursor getXRec() {
		if (xRec == null) {
			try {
				initXRec();
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

	protected abstract void _setFieldValue(String name, Object value);

	protected abstract Cursor _getBufferCopy() throws CelestaException;

	protected abstract Object[] _currentKeyValues();

	protected abstract void _setAutoIncrement(int val);

	protected abstract void _preDelete();

	protected abstract void _postDelete();

	protected abstract void _preUpdate();

	protected abstract void _postUpdate();

	protected abstract void _preInsert();

	protected abstract void _postInsert();

	// CHECKSTYLE:ON
}
