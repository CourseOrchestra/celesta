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
import java.util.stream.Collectors;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.stmt.MaskedStatementHolder;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.stmt.PreparedStatementHolderFactory;
import ru.curs.celesta.dbutils.stmt.PreparedStmtHolder;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

/**
 * Cursor class for data modification in tables.
 */
public abstract class Cursor extends BasicCursor implements InFilterSupport {

    private Table meta = null;
    final CursorGetHelper getHelper;
    private InFilterHolder inFilterHolder;

    final MaskedStatementHolder insert = PreparedStatementHolderFactory.createInsertHolder(meta(), db(), conn());

    boolean[] updateMask = null;
    boolean[] nullUpdateMask = null;
    final PreparedStmtHolder update = PreparedStatementHolderFactory.createUpdateHolder(
            meta(), db(), conn(), () -> updateMask, () -> nullUpdateMask
    );

    final PreparedStmtHolder delete = new PreparedStmtHolder() {

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            WhereTerm where = WhereTermsMaker.getPKWhereTerm(meta());
            where.programParams(program, db());
            return db().getDeleteRecordStatement(conn(), meta(), where.getWhere());
        }

    };

    private final PreparedStmtHolder deleteAll = new PreparedStmtHolder() {

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            WhereTerm where = getQmaker().getWhereTerm();
            where.programParams(program, db());
            return db().deleteRecordSetStatement(conn(), meta(), where.getWhere());
        }

    };

    private Cursor xRec;
    private int recversion;

    public Cursor(CallContext context) {
        super(context);
        CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
        cghb.withDb(db())
                .withConn(conn())
                .withMeta(meta())
                .withTableName(_objectName());

        getHelper = cghb.build();
        inFilterHolder = new InFilterHolder(this);
    }

    public Cursor(CallContext context, ColumnMeta<?>... columns) {
        this(context, Arrays.stream(columns).map(ColumnMeta::getName).collect(Collectors.toSet()));
    }

    public Cursor(CallContext context, Set<String> fields) {
        super(context, fields);

        CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
        cghb.withDb(db())
                .withConn(conn())
                .withMeta(meta())
                .withTableName(_objectName())
                .withFields(fieldsForStatement);

        getHelper = cghb.build();
        inFilterHolder = new InFilterHolder(this);
    }

    /**
     * Creates a table specific cursor.
     *
     * @param table       Cursor related table
     * @param callContext Call context that is used for cursor creation
     * @return
     */
    public static Cursor create(Table table, CallContext callContext) {
        return (Cursor) BasicCursor.create(table, callContext);
    }

    /**
     * Creates a table specific cursor.
     *
     * @param table       Cursor related table
     * @param callContext Call context that is used for cursor creation
     * @param fields      Fields the cursor should operate on
     * @return
     */
    public static Cursor create(Table table, CallContext callContext, Set<String> fields) {
        return (Cursor) BasicCursor.create(table, callContext, fields);
    }

    @Override
    PreparedStmtHolder getHereHolder() {
        return new PreparedStmtHolder() {
            @Override
            protected PreparedStatement initStatement(List<ParameterSetter> program) {
                WhereTerm where = getQmaker().getHereWhereTerm(meta());
                where.programParams(program, db());
                return db().getNavigationStatement(
                        conn(), getFrom(), "", where.getWhere(), fieldsForStatement, 0
                );
            }
        };
    }

    @Override
    protected void closeInternal() {
        super.closeInternal();
        if (xRec != null) {
            xRec.close();
        }
        closeStatements(getHelper.getHolder(), insert, delete, update);
    }

    /**
     * Performs cursor insert into the DB.
     */
    public final void insert() {
        if (!tryInsert()) {
            StringBuilder sb = new StringBuilder();
            for (Object value : _currentKeyValues()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(value == null ? "null" : value.toString());
            }
            throw new CelestaException("Record %s (%s) already exists", _objectName(), sb.toString());
        }
    }

    /**
     * Tries to perform cursor insert into the DB.
     *
     * @return {@code TRUE} if inserted successfully, otherwise - {@code FALSE}.
     */
    public final boolean tryInsert() {
        if (!canInsert()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.INSERT);
        }

        preInsert();
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
                    if (getRecversion() == 0) {
                        setRecversion(xRec.getRecversion());
                    }
                    return false;
                }
            } finally {
                rs.close();
            }

            PreparedStatement ins = insert.getStatement(_currentValues(), recversion);

            ILoggingManager loggingManager = callContext().getLoggingManager();
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
                for (Column<?> c : meta().getColumns().values()) {
                    if (c instanceof IntegerColumn) {
                        IntegerColumn ic = (IntegerColumn) c;
                        if (ic.getSequence() != null) {
                            _setAutoIncrement(db().getCurrentIdent(conn(), meta()));
                            break;
                        }
                    }
                }
            }

            getHelper.internalGet(this::_parseResultInternal, Optional.of(this::initXRec),
                    recversion, _currentKeyValues());

            postInsert();

        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
        return true;
    }

    /**
     * Performs an update of the cursor content in the DB, throwing an exception
     * in case if a record with such key fields is not found.
     */
    public final void update() {
        if (!tryUpdate()) {
            String values = Arrays.stream(_currentKeyValues())
                    .map(String::valueOf)
                    .collect(Collectors.joining(", "));

            throw new CelestaException("Record %s (%s) does not exist.", _objectName(), values);
        }
    }

    /**
     * Tries to perform an update of the cursor content in the DB.
     *
     * @return {@code TRUE} if updated successfully, otherwise - {@code FALSE}.
     */
    // CHECKSTYLE:OFF for cyclomatic complexity
    public final boolean tryUpdate() {
        // CHECKSTYLE:ON
        if (!canModify()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.MODIFY);
        }

        preUpdate();
        PreparedStatement g = getHelper.prepareGet(recversion, _currentKeyValues());
        try {
            ResultSet rs = g.executeQuery();
            try {
                if (!rs.next()) {
                    return false;
                }
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
            if (notChanged) {
                return true;
            }

            if (!(Arrays.equals(myMask, updateMask) && Arrays.equals(myNullsMask, nullUpdateMask))) {
                update.close();
                updateMask = myMask;
                nullUpdateMask = myNullsMask;
            }

            // for a completely new record
            if (getRecversion() == 0) {
                setRecversion(xRec.getRecversion());
            }

            PreparedStatement upd = update.getStatement(values, recversion);

            upd.execute();
            ILoggingManager loggingManager = callContext().getLoggingManager();
            loggingManager.log(this, Action.MODIFY);
            if (meta().isVersioned()) {
                recversion++;
            }
            this.initXRec();
            postUpdate();

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
     * Compares the values in order to find: what exactly was changed in the record.
     *
     * @param newVal new value
     * @param oldVal old value
     * @return {@code true} if the values were not changed, otherwise - {@code false}
     */
    private static boolean compareValues(Object newVal, Object oldVal) {
        if (newVal == null) {
            return oldVal == null || (oldVal instanceof BLOB);
        }
        if (newVal instanceof BLOB) {
            return !((BLOB) newVal).isModified();
        }
        return newVal.equals(oldVal);
    }

    /**
     * Deletes current record.
     */
    public final void delete() {
        if (!canDelete()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.DELETE);
        }

        PreparedStatement del = delete.getStatement(_currentValues(), recversion);

        try {
            preDelete();
            del.execute();
            ILoggingManager loggingManager = callContext().getLoggingManager();
            loggingManager.log(this, Action.DELETE);
            this.initXRec();
            postDelete();
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    private void initXRec() {
        if (xRec == null) {
            xRec = (Cursor) _getBufferCopy(callContext(), null);
        } else {
            xRec.copyFieldsFrom(this);
        }
    }

    /**
     * Deletes all records that were caught by current filter.
     */
    public final void deleteAll() {
        if (!canDelete()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.DELETE);
        }
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
     * Performs a search of a record by key fields, throwing an exception if
     * the record is not found.
     *
     * @param values values of the key fields
     */
    public final void get(Object... values) {
        if (!tryGet(values)) {
            StringBuilder sb = new StringBuilder();
            for (Object value : values) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(value == null ? "null" : value.toString());
            }
            throw new CelestaException("There is no %s (%s).", _objectName(), sb.toString());
        }
    }

    /**
     * Tries to perform a search of a record by the key fields, returning a value whether
     * the record was found or not.
     *
     * @param values values of the key fields
     * @return {@code true} if the record is found, otherwise - {@code false}
     */
    public final boolean tryGet(Object... values) {
        if (!canRead()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.READ);
        }
        return getHelper.internalGet(this::_parseResultInternal, Optional.of(this::initXRec),
                recversion, values);
    }

    /**
     * Retrieves a record from the database that corresponds to the fields of current
     * primary key.
     *
     * @return {@code true} if the record is retrieved, otherwise - {@code false}
     */
    public final boolean tryGetCurrent() {
        if (!canRead()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.READ);
        }
        return getHelper.internalGet(this::_parseResultInternal, Optional.of(this::initXRec),
                recversion, _currentKeyValues());
    }


    /**
     * Sets version of the record.
     *
     * @param v new version.
     */
    public final void setRecversion(int v) {
        recversion = v;
    }

    /**
     * Returns version of the record.
     *
     * @return
     */
    public final int getRecversion() {
        return recversion;
    }

    /**
     * Reads the content of BLOB field to memory.
     *
     * @param name field name
     * @return
     */
    protected BLOB calcBlob(String name) {
        ColumnMeta<?> c = validateColumnName(name);
        if (!(c instanceof BinaryColumn)) {
            throw new CelestaException("'%s' is not a BLOB column.", c.getName());
        }

        BinaryColumn bc = (BinaryColumn) c;

        BLOB result;

        List<ParameterSetter> program = new ArrayList<>();

        WhereTerm w = WhereTermsMaker.getPKWhereTerm(meta);
        PreparedStatement stmt = db().getOneFieldStatement(conn(), bc, w.getWhere());
        int i = 1;
        w.programParams(program, db());
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
     * Returns maximal length of the text field (if it is defined).
     *
     * @param name name of the text field
     * @return length of the text field or -1 (minus one) if MAX is indicated instead of the length.
     */
    @Deprecated
    public final int getMaxStrLen(String name) {
        ColumnMeta<?> c = validateColumnName(name);
        if (c instanceof StringColumn) {
            StringColumn sc = (StringColumn) c;
            return getMaxStrLen(sc);
        } else {
            throw new CelestaException("Column %s is not of string type.", c.getName());
        }
    }

    /**
     * Returns maximal length of the text field .
     *
     * @param column the text field
     * @return length of the text field or -1 (minus one) for TEXT fields.
     */
   public final int getMaxStrLen(ColumnMeta<String> column) {
        final int undefinedMaxlength = -1;
        if (column instanceof StringColumn) {
            StringColumn sc = (StringColumn) column;
            return sc.isMax() ? undefinedMaxlength : sc.getLength();
        } else {
            return undefinedMaxlength;
        }
    }

    /**
     * Resets all fields of the buffer except for the key ones.
     */
    public final void init() {
        _clearBuffer(false);
        setRecversion(0);
        if (xRec != null) {
            xRec.close();
        }
        xRec = null;
    }

    /**
     * Returns table description (meta information).
     *
     * @return
     */
    @Override
    public final Table meta() {
        if (meta == null) {
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_objectName(), Table.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        }
        return meta;
    }

    @Override
    final void appendPK(List<String> l, List<Boolean> ol, final Set<String> colNames) {
        // Always add to the end of OrderBy the fields of the primary key following in
        // a natural order.
        for (String colName : meta().getPrimaryKey().keySet()) {
            if (!colNames.contains(colName)) {
                l.add(String.format("\"%s\"", colName));
                ol.add(Boolean.FALSE);
            }
        }
    }

    @Override
    public final void clear() {
        super.clear();
        setRecversion(0);
        if (xRec != null) {
            xRec.close();
        }
        xRec = null;
    }

    /**
     * Returns a copy of the buffer containing values that were received by the
     * last read from the database.
     *
     * @return
     */
    public final Cursor getXRec() {
        if (xRec == null) {
            try {
                this.initXRec();
                xRec.clear();
            } catch (CelestaException e) {
                xRec = null;
            }
        }
        return xRec;
    }

    @Override
    public FieldsLookup setIn(BasicCursor otherCursor) {
        return inFilterHolder.setIn(otherCursor);
    }

    @Override
    public In getIn() {
        return inFilterHolder.getIn();
    }

    @Override
    protected void resetSpecificState() {
        inFilterHolder = new InFilterHolder(this);
    }

    @Override
    protected void clearSpecificState() {
        inFilterHolder = new InFilterHolder(this);
    }

    @Override
    protected void copySpecificFiltersFrom(BasicCursor bc) {
        Cursor c = (Cursor) bc;
        inFilterHolder = c.inFilterHolder;
    }

    @Override
    boolean isEquivalentSpecific(BasicCursor bc) {
        Cursor c = (Cursor) bc;
        return Objects.equals(inFilterHolder, c.inFilterHolder);
    }

    /**
     * Returns an array of field values of the primary key.
     *
     * @return
     */
    public final Object[] getCurrentKeyValues() {
        return _currentKeyValues();
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        this._parseResultInternal(rs);
        this.initXRec();
    }

    private void preDelete() {
        callContext().getCelesta().getTriggerDispatcher().fireTrigger(TriggerType.PRE_DELETE, this);
    }

    private void postDelete() {
        callContext().getCelesta().getTriggerDispatcher().fireTrigger(TriggerType.POST_DELETE, this);
    }

    private void preUpdate() {
        callContext().getCelesta().getTriggerDispatcher().fireTrigger(TriggerType.PRE_UPDATE, this);
    }

    private void postUpdate() {
        callContext().getCelesta().getTriggerDispatcher().fireTrigger(TriggerType.POST_UPDATE, this);
    }

    private void preInsert() {
        callContext().getCelesta().getTriggerDispatcher().fireTrigger(TriggerType.PRE_INSERT, this);
    }

    private void postInsert() {
        callContext().getCelesta().getTriggerDispatcher().fireTrigger(TriggerType.POST_INSERT, this);
    }

    /*
     * This group of methods is named according to Python rules, and not Java.
     * In Python names of protected methods are started with an underscore symbol.
     * When using methods without an underscore symbol conflicts with attribute names
     * may happen.
     */

    protected abstract Object[] _currentKeyValues();

    protected abstract void _setAutoIncrement(int val);

    protected abstract void _parseResultInternal(ResultSet rs) throws SQLException;

}
