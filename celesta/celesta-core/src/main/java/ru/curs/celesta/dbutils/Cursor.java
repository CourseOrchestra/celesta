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
import java.util.*;
import java.util.function.Function;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.stmt.MaskedStatementHolder;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.stmt.PreparedStmtHolder;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;
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

	private Table meta = null;
	private final CursorGetHelper getHelper;
	private In inFilter;


	private final MaskedStatementHolder insert = new MaskedStatementHolder() {

		@Override
		protected int[] getNullsMaskIndices() throws CelestaException {
			// we monitor all columns for nulls
			int[] result = new int[meta().getColumns().size()];
			for (int i = 0; i < result.length; i++)
				result[i] = i;
			return result;
		}

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			return db().getInsertRecordStatement(conn(), meta(), getNullsMask(), program);
		}

	};

	private boolean[] updateMask = null;
	private boolean[] nullUpdateMask = null;
	private final PreparedStmtHolder update = new PreparedStmtHolder() {
		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			WhereTerm where = WhereTermsMaker.getPKWhereTerm(meta());
			PreparedStatement result = db().getUpdateRecordStatement(conn(), meta(), updateMask, nullUpdateMask, program,
					where.getWhere());
			where.programParams(program);
			return result;
		}
	};

	private final PreparedStmtHolder delete = new PreparedStmtHolder() {

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			WhereTerm where = WhereTermsMaker.getPKWhereTerm(meta());
			where.programParams(program);
			return db().getDeleteRecordStatement(conn(), meta(), where.getWhere());
		}

	};

	private final PreparedStmtHolder deleteAll = new PreparedStmtHolder() {

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			WhereTerm where = getQmaker().getWhereTerm();
			where.programParams(program);
			return db().deleteRecordSetStatement(conn(), meta(), where.getWhere());
		}

	};

	private Cursor xRec;
	private int recversion;

	public Cursor(CallContext context) throws CelestaException {
		super(context);
		CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
		cghb.withDb(db())
				.withConn(conn())
				.withMeta(meta())
				.withTableName(_tableName());

		getHelper = cghb.build();
	}

	public Cursor(CallContext context, Set<String> fields) throws CelestaException {
		super(context, fields);

		CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
		cghb.withDb(db())
				.withConn(conn())
				.withMeta(meta())
				.withTableName(_tableName())
				.withFields(fieldsForStatement);

		getHelper = cghb.build();
	}

	@Override
	PreparedStmtHolder getHereHolder() {
		return new PreparedStmtHolder() {
			@Override
			protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
				WhereTerm where = getQmaker().getHereWhereTerm(meta());
				where.programParams(program);
				return db().getNavigationStatement(
						conn(), getFrom(),"", where.getWhere(), fieldsForStatement, 0
				);
			}
		};
	}

	@Override
	public final void close() {
		super.close();
		if (xRec != null)
			xRec.close();
		close(getHelper.getHolder(), insert, delete, update);
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
			throw new CelestaException("Record %s (%s) already exists", _tableName(), sb.toString());
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
			throw new PermissionDeniedException(callContext(), meta(), Action.INSERT);

		_preInsert();
		//TODO: одно из самых нуждающихся в переделке мест.
		// на один insert--2 select-а, что вызывает справедливое возмущение тех, кто смотрит логи
		// 1) Если у нас автоинкремент и автоинкрементное поле в None, то первый select не нужен
		// 2) Хорошо бы результат инсерта выдавать в одной операции как resultset 
 		PreparedStatement g = getHelper.prepareGet(recversion, _currentKeyValues());
		try {
			ResultSet rs = g.executeQuery();
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

			PreparedStatement ins = insert.getStatement(_currentValues(), recversion);

			LoggingManager loggingManager = callContext().getLoggingManager();
			if (ins.execute()) {
				loggingManager.log(this, Action.INSERT);
				ResultSet ret = ins.getResultSet();
				ret.next();
				int id = ret.getInt(1);
				_setAutoIncrement(id);
				ret.close();
			} else {
				// TODO: get rid of "getCurrentIdent" call where possible
				// e. g. using INSERT.. OUTPUT clause for MSSQL
				loggingManager.log(this, Action.INSERT);
				for (Column c : meta().getColumns().values())
					if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
						_setAutoIncrement(db().getCurrentIdent(conn(), meta()));
						break;
					}
			}

				getHelper.internalGet(this::_parseResult, this::initXRec,
						recversion, _currentKeyValues());
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
			throw new CelestaException("Record %s (%s) does not exist.", _tableName(), sb.toString());
		}
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
			throw new PermissionDeniedException(callContext(), meta(), Action.MODIFY);

		_preUpdate();
		PreparedStatement g = getHelper.prepareGet(recversion, _currentKeyValues());
		try {
			ResultSet rs = g.executeQuery();
			try {
				if (!rs.next())
					return false;
				// Прочитали из базы данных значения -- обновляем xRec
				if (xRec == null) {
					xRec = (Cursor) _getBufferCopy(callContext(), null);
					// Вопрос на будущее: эта строчка должна быть здесь или за
					// фигурной скобкой? (проблема совместной работы над базой)
					xRec._parseResult(rs);
				}
			} finally {
				rs.close();
			}

			Object[] values = _currentValues();
			Object[] xValues = getXRec()._currentValues();
			// Маска: true для тех случаев, когда поле не было изменено
			boolean[] myMask = new boolean[values.length];
			boolean[] myNullsMask = new boolean[values.length];
			boolean notChanged = true;
			for (int i = 0; i < values.length; i++) {
				myMask[i] = compareValues(values[i], xValues[i]);
				notChanged &= myMask[i];
				myNullsMask[i] = values[i] == null;
			}
			// Если ничего не изменилось -- выполнять дальнейшие действия нет
			// необходимости
			if (notChanged)
				return true;

			if (!(Arrays.equals(myMask, updateMask) && Arrays.equals(myNullsMask, nullUpdateMask))) {
				update.close();
				updateMask = myMask;
				nullUpdateMask = myNullsMask;
			}

			// for a completely new record
			if (getRecversion() == 0)
				setRecversion(xRec.getRecversion());

			PreparedStatement upd = update.getStatement(values, recversion);

			upd.execute();
			LoggingManager loggingManager = callContext().getLoggingManager();
			loggingManager.log(this, Action.MODIFY);
			if (meta().isVersioned())
				recversion++;
			initXRec();
			_postUpdate();

		} catch (SQLException e) {
			if (e.getMessage().contains("record version check failure")) {
				throw new CelestaException(
						"Can not update %s.%s(%s): this record has been already modified"
								+ " by someone. Please start updating again.",
						meta().getGrain().getName(), meta().getName(), Arrays.toString(_currentKeyValues()));
			} else {
				throw new CelestaException("Update of %s.%s (%s) failure: %s", meta().getGrain().getName(),
						meta().getName(), Arrays.toString(_currentKeyValues()), e.getMessage());
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
			throw new PermissionDeniedException(callContext(), meta(), Action.DELETE);

		PreparedStatement del = delete.getStatement(_currentValues(), recversion);

		try {
			_preDelete();
			del.execute();
			LoggingManager loggingManager = callContext().getLoggingManager();
			loggingManager.log(this, Action.DELETE);
			initXRec();
			_postDelete();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	final void initXRec() throws CelestaException {
		if (xRec == null) {
			xRec = (Cursor) _getBufferCopy(callContext(), null);
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
			throw new PermissionDeniedException(callContext(), meta(), Action.DELETE);
		PreparedStatement stmt = deleteAll.getStatement(_currentValues(), recversion);
		try {
			try {
				stmt.executeUpdate();
			} finally {
				deleteAll.close();
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
			throw new CelestaException("There is no %s (%s).", _tableName(), sb.toString());
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
			throw new PermissionDeniedException(callContext(), meta(), Action.READ);
		return getHelper.internalGet(this::_parseResult, this::initXRec,
				recversion, values);
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
			throw new PermissionDeniedException(callContext(), meta(), Action.READ);
		return getHelper.internalGet(this::_parseResult, this::initXRec,
				recversion, _currentKeyValues());
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
			throw new CelestaException("'%s' is not a BLOB column.", c.getName());
		BLOB result;

		List<ParameterSetter> program = new ArrayList<>();

		WhereTerm w = WhereTermsMaker.getPKWhereTerm(meta);
		PreparedStatement stmt = db().getOneFieldStatement(conn(), c, w.getWhere());
		int i = 1;
		w.programParams(program);
		Object[] rec = _currentValues();
		for (ParameterSetter f : program) {
			f.execute(stmt, i++, rec, recversion);
		}

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
			throw new CelestaException("Column %s is not of string type.", c.getName());
		}
	}

	/**
	 * Очистка всех полей буфера, кроме ключевых.
	 */
	public final void init() {
		_clearBuffer(false);
		setRecversion(0);
		if (xRec != null)
			xRec.close();
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
				meta = callContext().getScore()
						.getGrain(_grainName()).getElement(_tableName(), Table.class);
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
		return meta;
	}

	@Override
	final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) throws CelestaException {
		// Всегда добавляем в конец OrderBy поля первичного ключа, идующие в
		// естественном порядке
		for (String colName : meta().getPrimaryKey().keySet())
			if (!colNames.contains(colName)) {
				l.add(String.format("\"%s\"", colName));
				ol.add(Boolean.FALSE);
			}
	}

	@Override
	public final void clear() throws CelestaException {
		super.clear();
		setRecversion(0);
		if (xRec != null)
			xRec.close();
		xRec = null;
	}

	/**
	 * Возвращает копию буфера, содержащую значения, полученные при последнем
	 * чтении данных из базы.
	 */
	public final BasicCursor getXRec() {
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

	public final FieldsLookup setIn(Cursor otherCursor) throws CelestaException {

		Runnable lookupChangeCallback = () -> {
			try {
				if (!isClosed())
					// пересоздаём набор
					closeSet();
			} catch (CelestaException e) {
				throw new RuntimeException();
			}
		};

		Function<FieldsLookup, Void> newLookupCallback = (lookup) -> {
			inFilter.addLookup(lookup, lookup.getOtherCursor().getQmaker());
			return null;
		};

		FieldsLookup fieldsLookup = new FieldsLookup(this, otherCursor, lookupChangeCallback, newLookupCallback);
		WhereTermsMaker otherWhereTermMaker = otherCursor.getQmaker();
		inFilter = new In(fieldsLookup, otherWhereTermMaker);

		return fieldsLookup;
	}

	@Override
	protected In getIn() {
		return inFilter;
	}

	@Override
	protected void resetSpecificState() {
		inFilter = null;
	}

	@Override
	protected void clearSpecificState() {
		inFilter = null;
	}

	@Override
	protected void copySpecificFiltersFrom(BasicCursor bc) {
		Cursor c = (Cursor) bc;
		inFilter = c.inFilter;
	}

	@Override
	boolean isEquivalentSpecific(BasicCursor bc) throws CelestaException {
		Cursor c = (Cursor) bc;
		return Objects.equals(inFilter, c.inFilter);
	}

	/**
	 * Устанавливает текущее значение счётчика IDENTITY на таблице (если он
	 * есть). Этот метод предназначен для реализации механизмов экспорта-импорта
	 * данных из таблицы. Его следует применять с осторожностью, т.к. сбой в
	 * отсчёте IDENTIY-счётчика может привести к нарушению первичного ключа.
	 * Кроме того, как минимум в Oracle, в силу особенностей реализации, не
	 * гарантируется надёжная работа этого метода в условиях конкурретного
	 * доступа к таблице.
	 * 
	 * @param newValue
	 *            значение, которое должно принять поле IDENITITY при следующей
	 *            вставке.
	 * @throws CelestaException
	 *             Если таблица не содержит IDENTITY-поля или в случае сбоя
	 *             работы с базой данных.
	 */
	public final void resetIdentity(int newValue) throws CelestaException {
		IntegerColumn ic = DBAdaptor.findIdentityField(meta());
		if (ic == null)
			throw new CelestaException("Cannot reset identity: there is no IDENTITY field defined for table %s.%s.",
					_grainName(), _tableName());

		try {
			db().resetIdentity(conn(), meta(), newValue);
		} catch (SQLException e) {
			throw new CelestaException("Cannot reset identity for table %s.%s with message '%s'.", _grainName(),
					_tableName(), e.getMessage());
		}
	}

	/**
	 * Возвращает в массиве значения полей первичного ключа.
	 */
	public Object[] getCurrentKeyValues() {
		return _currentKeyValues();
	}

	// CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */

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
