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
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cursor class for data modification in tables.
 */
public abstract class Cursor extends BasicCursor implements InFilterSupport {

    final MaskedStatementHolder insert = PreparedStatementHolderFactory.createInsertHolder(meta(), db(), conn());
    final CursorGetHelper getHelper;
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
    private Table meta = null;
    private InFilterHolder inFilterHolder;
    private final PreparedStmtHolder deleteAll = new PreparedStmtHolder() {

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            WhereTerm where = getQmaker().getWhereTerm();
            where.programParams(program, db());
            return db().deleteRecordSetStatement(conn(), meta(), where.getWhere());
        }

    };
    private byte canOptimizeInsertion;
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

    Cursor(CallContext context, Set<String> fields) {
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
            throw new CelestaException("Record %s %s already exists",
                    _objectName(),
                    Arrays.toString(_currentKeyValues()));
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

        try {
            //Pre-insertion select: we need to check if the record already exists
            //and if it does, populate the cursor's XRec with the existing values
            //and return `false`.
            //However, in case when we have a null value for auto-incremented PK,
            //we are guaranteed to insert a fresh record, so this can be skipped.
            if (!canOptimizeInsertion()) {
                try (PreparedStatement g = getHelper.prepareGet(recversion, _currentKeyValues());
                     ResultSet rs = g.executeQuery()) {
                    if (rs.next()) {
                        getXRec()._parseResult(rs);
                        //transmit recversion from xRec to rec for possible future
                        //record update
                        if (getRecversion() == 0) {
                            setRecversion(xRec.getRecversion());
                        }
                        return false;
                    }
                }
            }

            PreparedStatement ins = insert.getStatement(_currentValues(), recversion);

            ILoggingManager loggingManager = callContext().getLoggingManager();
            if (ins.execute()) {
                loggingManager.log(this, Action.INSERT);
                try (ResultSet ret = ins.getResultSet()) {
                    ret.next();
                    int id = ret.getInt(1);
                    _setAutoIncrement(id);
                }
            } else {
                loggingManager.log(this, Action.INSERT);
                meta().getAutoincrementedColumn().ifPresent(
                        // Post-insertion select to get the value of auto-incremented field.
                        // NB: this is currently needed only for Oracle (in all the cases)
                        // and MS SQL Server (for the case when there are MViews for the table,
                        // as insert..output does not work in MS SQL in this scenario).
                        // In all other scenarios, we are using the value returned by the
                        // insertion command (like select..returning in PostgreSQL).
                        ic -> _setAutoIncrement(db().getCurrentIdent(conn(), meta())));
            }

            getHelper.internalGet(this::_parseResultInternal, Optional.of(this::initXRec),
                    recversion, _currentKeyValues());

            postInsert();

        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
        return true;
    }

    final boolean canOptimizeInsertion() {
        /*If the only key value is an auto-incremented integer,
         * and the inserted value is null, then we can skip the selection phase.*/
        status:
        if (canOptimizeInsertion == 0) {
            Collection<Column<?>> pkColumns = meta().getPrimaryKey().values();
            if (pkColumns.size() == 1) {
                Column<?> pkColumn = pkColumns.iterator().next();
                if (pkColumn instanceof IntegerColumn) {
                    IntegerColumn intPkColumn = (IntegerColumn) pkColumn;
                    canOptimizeInsertion = intPkColumn.getSequence() == null ? (byte) 1 : (byte) 2;
                    break status;
                }
            }
            canOptimizeInsertion = 1;
        }
        return canOptimizeInsertion == 2 && getCurrentKeyValues()[0] == null;
    }

    /**
     * Performs an update of the cursor content in the DB, throwing an exception
     * in case if a record with such key fields is not found.
     */
    public final void update() {
        if (!tryUpdate()) {
            throw new CelestaException("Record %s %s does not exist.",
                    _objectName(),
                    Arrays.toString(_currentKeyValues()));
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
            try (ResultSet rs = g.executeQuery()) {
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
            return oldVal == null || oldVal instanceof BLOB;
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
            throw new CelestaException(e.getMessage(), e);
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
            throw new CelestaException(e.getMessage(), e);
        }
    }

    /**
     * Performs a search of a record by key fields, throwing an exception if
     * the record is not found.
     *
     * @param values values of the key fields
     */
    public final void getByValuesArray(Object... values) {
        if (!tryGetByValuesArray(values)) {
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
    public boolean tryGetByValuesArray(Object... values) {
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
        try (PreparedStatement stmt = db().getOneFieldStatement(conn(), bc, w.getWhere())) {
            int i = 1;
            w.programParams(program, db());
            Object[] rec = _currentValues();
            for (ParameterSetter f : program) {
                f.execute(stmt, i++, rec, recversion);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream is = rs.getBinaryStream(1)) {
                        if (!(is == null || rs.wasNull())) {
                            result = new BLOB(is);
                        } else {
                            // Field's value is NULL
                            result = new BLOB();
                        }
                    }
                } else {
                    // There is no record at all
                    result = new BLOB();
                }
            }
        } catch (SQLException | IOException e) {
            throw new CelestaException(e.getMessage(), e);
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
    @SuppressWarnings("MethodName")
    protected abstract Object[] _currentKeyValues();

    @SuppressWarnings("MethodName")
    protected abstract void _setAutoIncrement(int val);

    @SuppressWarnings("MethodName")
    protected abstract void _parseResultInternal(ResultSet rs) throws SQLException;

}
