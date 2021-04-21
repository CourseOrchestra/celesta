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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.dbutils.filter.AbstractFilter;
import ru.curs.celesta.dbutils.filter.Filter;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.Range;
import ru.curs.celesta.dbutils.filter.SingleValue;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.MaskedStatementHolder;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.stmt.PreparedStatementHolderFactory;
import ru.curs.celesta.dbutils.stmt.PreparedStmtHolder;
import ru.curs.celesta.dbutils.term.FromTerm;
import ru.curs.celesta.dbutils.term.WhereMakerParamsProvider;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;
import ru.curs.celesta.score.CelestaParser;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.ParseException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Base cursor class for reading data from views.
 */
public abstract class BasicCursor extends BasicDataAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCursor.class);

    private static final String DATABASE_CLOSING_ERROR =
            "Database error when closing recordset for table '%s': %s";
    private static final String NAVIGATING_ERROR = "Error while navigating cursor: %s";

    private static final Pattern COLUMN_NAME =
            Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)( +([Aa]|[Dd][Ee])[Ss][Cc])?");

    private static final Pattern NAVIGATION = Pattern.compile("[+-<>=]+");
    private static final Pattern NAVIGATION_WITH_OFFSET = Pattern.compile("[<>]");

    protected Set<String> fields = Collections.emptySet();
    protected Set<String> fieldsForStatement = Collections.emptySet();

    protected ResultSet cursor;

    protected FromTerm fromTerm;

    final PreparedStmtHolder set = PreparedStatementHolderFactory.createFindSetHolder(
            BasicCursor.this.db(),
            BasicCursor.this.conn(),
            //NB: do not replace with method reference, this will cause NPE in initializer
            () -> BasicCursor.this.getFrom(),
            () -> {
                if (BasicCursor.this.fromTerm == null) {
                    BasicCursor.this.fromTerm = new FromTerm(BasicCursor.this.getFrom().getParameters());
                    return BasicCursor.this.fromTerm;
                }
                return BasicCursor.this.fromTerm;
            },
            () -> BasicCursor.this.qmaker.getWhereTerm(),
            () -> BasicCursor.this.getOrderBy(),
            () -> BasicCursor.this.offset,
            () -> BasicCursor.this.rowCount,
            () -> BasicCursor.this.fieldsForStatement
    );

    final PreparedStmtHolder count = new PreparedStmtHolder() {
        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            FromClause from = getFrom();

            if (fromTerm == null) {
                fromTerm = new FromTerm(from.getParameters());
            }

            WhereTerm where = qmaker.getWhereTerm();

            fromTerm.programParams(program, db());
            where.programParams(program, db());
            return db().getSetCountStatement(conn(), from, where.getWhere());
        }
    };

    /**
     * Base holder class for a number of queries that depend on null mask on
     * sort fields.
     */
    abstract class OrderFieldsMaskedStatementHolder extends MaskedStatementHolder {
        @Override
        protected final int[] getNullsMaskIndices() {
            if (orderByNames == null) {
                orderBy();
            }
            return orderByIndices;
        }
    }

    final PreparedStmtHolder position = new OrderFieldsMaskedStatementHolder() {
        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            FromClause from = getFrom();

            if (fromTerm == null) {
                fromTerm = new FromTerm(from.getParameters());
            }

            WhereTerm where = qmaker.getWhereTerm('<');
            fromTerm.programParams(program, db());
            where.programParams(program, db());
            return db().getSetCountStatement(conn(), getFrom(), where.getWhere());
        }

    };

    final PreparedStmtHolder forwards = new OrderFieldsMaskedStatementHolder() {
        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            FromClause from = getFrom();

            if (fromTerm == null) {
                fromTerm = new FromTerm(from.getParameters());
            }

            WhereTerm where = qmaker.getWhereTerm('>');
            fromTerm.programParams(program, db());
            where.programParams(program, db());
            return db().getNavigationStatement(
                    conn(), getFrom(), getOrderBy(), where.getWhere(), fieldsForStatement, navigationOffset
            );
        }

    };

    final PreparedStmtHolder backwards = new OrderFieldsMaskedStatementHolder() {

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            FromClause from = getFrom();

            if (fromTerm == null) {
                fromTerm = new FromTerm(from.getParameters());
            }

            WhereTerm where = qmaker.getWhereTerm('<');
            fromTerm.programParams(program, db());
            where.programParams(program, db());
            return db().getNavigationStatement(
                    conn(), getFrom(), getReversedOrderBy(), where.getWhere(), fieldsForStatement, navigationOffset
            );
        }

    };

    final PreparedStmtHolder here = getHereHolder();

    final PreparedStmtHolder first = new PreparedStmtHolder() {
        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            FromClause from = getFrom();

            if (fromTerm == null) {
                fromTerm = new FromTerm(from.getParameters());
            }

            WhereTerm where = qmaker.getWhereTerm();
            fromTerm.programParams(program, db());
            where.programParams(program, db());
            return db().getNavigationStatement(
                    conn(), getFrom(), getOrderBy(), where.getWhere(), fieldsForStatement, 0
            );
        }

    };

    final PreparedStmtHolder last = new PreparedStmtHolder() {
        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            FromClause from = getFrom();

            if (fromTerm == null) {
                fromTerm = new FromTerm(from.getParameters());
            }

            WhereTerm where = qmaker.getWhereTerm();
            fromTerm.programParams(program, db());
            where.programParams(program, db());
            return db().getNavigationStatement(
                    conn(), getFrom(), getReversedOrderBy(), where.getWhere(), fieldsForStatement, 0
            );
        }
    };

    // Filter and sort fields
    private final Map<String, AbstractFilter> filters = new HashMap<>();
    private String[] orderByNames;
    private int[] orderByIndices;
    private boolean[] descOrders;

    private long offset = 0;
    private long navigationOffset = 0;
    private long rowCount = 0;
    private Expr complexFilter;

    private final WhereTermsMaker qmaker = new WhereTermsMaker(new WhereMakerParamsProvider() {

        @Override
        public void initOrderBy() {
            if (orderByNames == null) {
                orderBy();
            }
        }

        @Override
        public QueryBuildingHelper dba() {
            return db();
        }

        @Override
        public String[] sortFields() {
            return orderByNames;
        }

        @Override
        public boolean[] descOrders() {
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
        public int[] sortFieldsIndices() {
            return orderByIndices;
        }

        @Override
        public Object[] values() {
            return _currentValues();
        }

        @Override
        public boolean isNullable(String columnName) {
            return meta().getColumns().get(columnName).isNullable();
        }
    });

    public BasicCursor(CallContext context) {
        super(context);
    }

    public BasicCursor(CallContext context, Set<String> fields) {
        this(context);
        if (!meta().getColumns().keySet().containsAll(fields)) {
            throw new CelestaException("Not all of specified columns exist!!!");
        }
        this.fields = fields;
        prepareOrderBy();
        fillFieldsForStatement();
    }

    static BasicCursor create(DataGrainElement element, CallContext callContext) {
        try {
            return getCursorClass(element).getConstructor(CallContext.class).newInstance(callContext);
        } catch (ReflectiveOperationException ex) {
            throw new CelestaException("Cursor creation failed for grain element: " + element.getName(), ex);
        }
    }

    static BasicCursor create(DataGrainElement element, CallContext callContext, Set<String> fields) {
        try {
            return getCursorClass(element)
                    .getConstructor(CallContext.class, Set.class).newInstance(callContext, fields);
        } catch (ReflectiveOperationException ex) {
            throw new CelestaException("Cursor creation failed for grain element: " + element.getName(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    static Class<? extends BasicCursor> getCursorClass(DataGrainElement element) throws ClassNotFoundException {
        final String namespace = element.getGrain().getNamespace().getValue();
        String cursorClassName =
                element.getName().substring(0, 1).toUpperCase() + element.getName().substring(1) + "Cursor";
        cursorClassName = (namespace.isEmpty() ? "" : namespace + ".") + cursorClassName;

        return (Class<? extends BasicCursor>) Class.forName(
                cursorClassName, true, Thread.currentThread().getContextClassLoader());
    }

    PreparedStmtHolder getHereHolder() {
        // To be overriden in Cursor class
        return new OrderFieldsMaskedStatementHolder() {

            @Override
            protected PreparedStatement initStatement(List<ParameterSetter> program) {
                WhereTerm where = qmaker.getWhereTerm('=');
                where.programParams(program, db());
                return db().getNavigationStatement(
                        conn(), getFrom(), "", where.getWhere(), fieldsForStatement, 0
                );
            }

        };
    }

    final void closeStatements(PreparedStmtHolder... stmts) {
        for (PreparedStmtHolder stmt : stmts) {
            stmt.close();
        }
    }

    /**
     * Releases all PreparedStatements of the cursor.
     */
    @Override
    protected void closeInternal() {
        super.closeInternal();
        closeStatements(set, forwards, backwards, here, first, last, count, position);
    }

    final Map<String, AbstractFilter> getFilters() {
        return filters;
    }

    @Override
    public abstract DataGrainElement meta();


    /**
     * Whether the session has rights to insert data into current table.
     *
     * @return
     */
    public final boolean canInsert() {
        if (isClosed()) {
            throw new CelestaException(DATA_ACCESSOR_IS_CLOSED);
        }
        IPermissionManager permissionManager = callContext().getPermissionManager();
        return permissionManager.isActionAllowed(callContext(), meta(), Action.INSERT);
    }

    /**
     * Whether the session has rights to modify data of current table.
     *
     * @return
     */
    public final boolean canModify() {
        if (isClosed()) {
            throw new CelestaException(DATA_ACCESSOR_IS_CLOSED);
        }
        IPermissionManager permissionManager = callContext().getPermissionManager();
        return permissionManager.isActionAllowed(callContext(), meta(), Action.MODIFY);
    }

    /**
     * Whether the session has rights to delete data from current table.
     *
     * @return
     */
    public final boolean canDelete() {
        if (isClosed()) {
            throw new CelestaException(DATA_ACCESSOR_IS_CLOSED);
        }
        IPermissionManager permissionManager = callContext().getPermissionManager();
        return permissionManager.isActionAllowed(callContext(), meta(), Action.DELETE);
    }

    private void closeStmt(PreparedStatement stmt) {
        try {
            stmt.close();
        } catch (SQLException e) {
            throw new CelestaException(DATABASE_CLOSING_ERROR, _objectName(), e.getMessage());
        }
    }

    protected final void closeSet() {
        cursor = null;
        set.close();
        forwards.close();
        backwards.close();
        first.close();
        last.close();
        count.close();
        position.close();
    }

    private String getOrderBy(boolean reverse) {
        if (orderByNames == null) {
            orderBy();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < orderByNames.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(orderByNames[i]);
            if (reverse ^ descOrders[i]) {
                sb.append(" desc");
            }
        }
        return sb.toString();

    }

    /**
     * Returns "order by" clause for the cursor.
     *
     * @return
     */
    public final String getOrderBy() {
        return getOrderBy(false);
    }

    final List<String> getOrderByFields() {
        if (orderByNames == null) {
            orderBy();
        }
        return Arrays.asList(orderByNames);
    }

    final String getReversedOrderBy() {
        return getOrderBy(true);
    }

    /**
     * Returns column names that are in sorting.
     *
     * @return
     */
    public String[] orderByColumnNames() {
        if (orderByNames == null) {
            orderBy();
        }
        return orderByNames;
    }

    /**
     * Returns mask of DESC orders.
     *
     * @return
     */
    public boolean[] descOrders() {
        if (orderByNames == null) {
            orderBy();
        }
        return descOrders;
    }

    /**
     * Moves to the first record in the filtered data set and returns information
     * about the success of transition.
     *
     * @return {@code true} if the transition was successful,
     * {@code false} if there are no records in the data set.
     */
    public final boolean tryFindSet() {
        if (!canRead()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.READ);
        }

        PreparedStatement ps = set.getStatement(_currentValues(), 0);
        boolean result;
        try {
            if (cursor != null) {
                cursor.close();
            }
            cursor = ps.executeQuery();
            result = cursor.next();
            if (result) {
                _parseResult(cursor);
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
        return result;
    }

    /**
     * The same as navigate("-").
     *
     * @return
     */
    public final boolean tryFirst() {
        return navigate("-");
    }

    /**
     * The same as tryFirst() but causes an error if no record is found.
     *
     * @return
     */
    public final void first() {
        if (!navigate("-")) {
            raiseNotFound();
        }
    }

    /**
     * The same as navigate("+").
     *
     * @return
     */
    public final boolean tryLast() {
        return navigate("+");
    }

    /**
     * The same as tryLast() but causes an error if no record is found.
     *
     * @return
     */
    public final void last() {
        if (!navigate("+")) {
            raiseNotFound();
        }
    }

    /**
     * The same as navigate("&gt;").
     *
     * @return
     */
    public final boolean next() {
        return navigate(">");
    }

    /**
     * The same as navigate("&lt").
     *
     * @return
     */
    public final boolean previous() {
        return navigate("<");
    }

    private void raiseNotFound() {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, AbstractFilter> e : filters.entrySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%s=%s", e.getKey(), e.getValue().toString()));
        }
        throw new CelestaException("There is no %s (%s).", _objectName(), sb.toString());
    }

    /**
     * Moves to the first record in the filtered data set causing an error in the case
     * if the transition was not successful.
     */
    public final void findSet() {
        if (!tryFindSet()) {
            raiseNotFound();
        }
    }

    /**
     * Returns current state of the cursor in form of CSV string with comma delimiters.
     *
     * @return
     */
    public final String asCSVLine() {
        Object[] values = _currentValues();
        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            if (value == null) {
                sb.append("NULL");
            } else {
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
                if (c == '"') {
                    sb.append('"');
                }
            }
            sb.append('"');
        } else {
            sb.append(fieldValue);
        }

    }

    /**
     * Moves to the next record in the sorted data set. Returns {@code false} if
     * the end of the set is reached.
     *
     * @return
     */
    public final boolean nextInSet() {
        boolean result;
        try {
            if (cursor == null) {
                result = tryFindSet();
            } else {
                result = cursor.next();
            }
            if (result) {
                _parseResult(cursor);
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
     * Navigation method (step-by-step transition in the filtered and sorted data set).
     *
     * @param command Command consisting of a sequence of symbols:
     *                <ul>
     *                <li>= update current record (if it exists in the filtered data set)
     *                <li>&gt; move to the next record in the filtered data set</li>
     *                <li>&lt; move to the previous record in the filtered data set</li>
     *                <li>- move to the first record in the filtered data set</li>
     *                <li>+ move to the last record in the filtered data set</li>
     *                </ul>
     * @return {@code true} if the record was found and the transition completed
     * {@code false} - otherwise.
     */
    public boolean navigate(String command) {
        if (!canRead()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.READ);
        }

        Matcher m = NAVIGATION.matcher(command);
        if (!m.matches()) {
            throw new CelestaException(
                    "Invalid navigation command: '%s', should consist of '+', '-', '>', '<' and '=' only!",
                    command);
        }

        if (navigationOffset != 0) {
            closeStatements(backwards, forwards);
        }

        navigationOffset = 0;
        for (int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);
            PreparedStatement navigator = chooseNavigator(c);

            if (executeNavigator(navigator)) {
                return true;
            }
        }
        return false;
    }

    public boolean navigate(String command, long offset) {
        if (!canRead()) {
            throw new PermissionDeniedException(callContext(), meta(), Action.READ);
        }

        Matcher m = NAVIGATION_WITH_OFFSET.matcher(command);
        if (!m.matches()) {
            throw new CelestaException(
                    "Invalid navigation command: '%s', should consist only one of  '>' or '<'!",
                    command);
        }

        if (offset < 0) {
            throw new CelestaException("Invalid navigation offset: offset should not be less than 0");
        }

        if (navigationOffset != offset) {
            navigationOffset = offset;
            closeStatements(backwards, forwards);
        }

        PreparedStatement navigator = chooseNavigator(command.charAt(0));
        LOGGER.trace("{}", navigator);
        return executeNavigator(navigator);

    }

    private boolean executeNavigator(PreparedStatement navigator) {
        try {
            LOGGER.trace("{}", navigator);
            try (ResultSet rs = navigator.executeQuery()) {
                if (rs.next()) {
                    _parseResult(rs);
                    return true;
                }
            }
        } catch (SQLException e) {
            throw new CelestaException(NAVIGATING_ERROR, e.getMessage());
        }
        return false;
    }

    private PreparedStatement chooseNavigator(char c) {
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

    final WhereTermsMaker getQmaker() {
        return qmaker;
    }

    final ColumnMeta<?> validateColumnName(String name) {
        ColumnMeta<?> column = meta().getColumns().get(name);
        if (column == null) {
            throw new CelestaException("No column %s exists in table %s.", name, _objectName());
        }

        return column;
    }

    private Object validateColumnValue(ColumnMeta<?> column, Object value) {
        if (value == null) {
            return value;
        }
        if (!column.getJavaClass().isAssignableFrom(value.getClass())) {
            throw new CelestaException("Value %s is not of type %s.", value, column.getJavaClass());
        }

        return value;
    }

    /**
     * Resets any filter on a field.
     *
     * @param name field name
     */
    @Deprecated
    public final void setRange(String name) {
        setRange(validateColumnName(name));
    }

    /**
     * Resets any filter on a field.
     *
     * @param column field column
     */
    public final void setRange(ColumnMeta<?> column) {
        validateColumnName(column.getName());
        if (isClosed()) {
            return;
        }
        // If filter was present on the field - reset the data set. If not - do nothing.
        if (filters.remove(column.getName()) != null) {
            closeSet();
        }
    }

    /**
     * Sets range from a single value on the field.
     *
     * @param name  field name
     * @param value value along which filtering is performed
     */
    @Deprecated
    public final void setRange(String name, Object value) {
        @SuppressWarnings("unchecked")
        ColumnMeta<Object> column = (ColumnMeta<Object>) validateColumnName(name);
        setRange(column, validateColumnValue(column, value));
    }

    /**
     * Sets range from a single value on the field.
     *
     * @param column field column
     * @param value  value along which filtering is performed
     * @param <T>    Java type of value
     */
    public final <T> void setRange(ColumnMeta<? super T> column, T value) {
        validateColumnName(column.getName());
        if (value == null) {
            setFilter(column, "null");
        } else {
            if (isClosed()) {
                return;
            }
            AbstractFilter oldFilter = filters.get(column.getName());
            // If one SingleValue is changed to another one - it is not needed
            // to close up the data set - the old one can be used.
            if (oldFilter instanceof SingleValue) {
                ((SingleValue) oldFilter).setValue(value);
            } else {
                filters.put(column.getName(), new SingleValue(value));
                closeSet();
            }
        }
    }

    /**
     * Sets range from..to on the field.
     *
     * @param name      field name
     * @param valueFrom value <em>from</em>
     * @param valueTo   value <em>to</em>
     */
    @Deprecated
    public final void setRange(String name, Object valueFrom, Object valueTo) {
        @SuppressWarnings("unchecked")
        ColumnMeta<Object> column = (ColumnMeta<Object>) validateColumnName(name);
        setRange(column, validateColumnValue(column, valueFrom), validateColumnValue(column, valueTo));
    }

    /**
     * Sets range from..to on the field.
     *
     * @param column    field column
     * @param valueFrom value <em>from</em>
     * @param valueTo   value <em>to</em>
     * @param <T>       Java type of value
     */
    public final <T> void setRange(ColumnMeta<? super T> column, T valueFrom, T valueTo) {
        validateColumnName(column.getName());
        if (isClosed()) {
            return;
        }
        AbstractFilter oldFilter = filters.get(column.getName());
        // If one Range is changed to another one - it is not needed
        // to close up the data set - the old one can be used.
        if (oldFilter instanceof Range) {
            ((Range) oldFilter).setValues(valueFrom, valueTo);
        } else {
            filters.put(column.getName(), new Range(valueFrom, valueTo));
            closeSet();
        }
    }

    /**
     * Sets filter to the field.
     *
     * @param name  field name
     * @param value filter
     */
    @Deprecated
    public final void setFilter(String name, String value) {
        setFilter(validateColumnName(name), value);
    }

    /**
     * Sets filter to the field.
     *
     * @param column field column
     * @param value  filter
     */
    public final void setFilter(ColumnMeta<?> column, String value) {
        validateColumnName(column.getName());
        if (value == null || value.isEmpty()) {
            throw new CelestaException(
                    "Filter for column %s is null or empty. "
                            + "Use setRange(column) to remove any filters from the column.",
                    column.getName());
        }
        AbstractFilter oldFilter = filters.put(column.getName(), new Filter(value, column));
        if (isClosed()) {
            return;
        }
        // If the old filter is changed towards the same - do nothing.
        if (!(oldFilter instanceof Filter && value.equals(oldFilter.toString()))) {
            closeSet();
        }
    }

    /**
     * Sets complex condition to the data set.
     *
     * @param condition condition that corresponds to WHERE clause.
     */
    public final void setComplexFilter(String condition) {
        Expr buf = CelestaParser.parseComplexFilter(condition, meta().getGrain().getScore().getIdentifierParser());
        try {
            buf.resolveFieldRefs(meta());
        } catch (ParseException e) {
            throw new CelestaException(e.getMessage());
        }
        complexFilter = buf;
        if (isClosed()) {
            return;
        }
        // recreate the data set
        closeSet();
    }

    /**
     * Returns (reformatted) expression of the complex filter in CelestaSQL dialect.
     */
    public final String getComplexFilter() {
        return complexFilter == null ? null : complexFilter.getCSQL();
    }

    /**
     * Sets filter to a range of records returned by the cursor.
     *
     * @param offset   number of records that has to be skipped (0 - start from the beginning).
     * @param rowCount maximal number of records that has to be returned (0 - return all records).
     */
    public final void limit(long offset, long rowCount) {
        if (offset < 0) {
            throw new CelestaException("Negative offset (%d) in limit(...) call", offset);
        }
        if (rowCount < 0) {
            throw new CelestaException("Negative rowCount (%d) in limit(...) call", rowCount);
        }
        this.offset = offset;
        this.rowCount = rowCount;
        closeSet();
    }

    /**
     * Resets filters and sorting.
     */
    public final void reset() {
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

    protected void resetSpecificState() {
    }

    /**
     * Sets sorting.
     *
     * @param names array of fields for sorting
     */
    @Deprecated
    public final void orderBy(String... names) {

        ColumnMeta<?>[] columns = new ColumnMeta<?>[names.length];
        for (int i = 0; i < names.length; i++) {
            final String name = names[i];
    
            Matcher m = COLUMN_NAME.matcher(name);
            if (!m.matches()) {
                throw new CelestaException(
                        "orderby() argument '%s' should match pattern <column name> [ASC|DESC]",
                        name);
            }

            final String colName = m.group(1);
            final String colOrdering = Optional.ofNullable(m.group(2)).map(String::trim).orElse(null);

            ColumnMeta<?> column = validateColumnName(colName);
            if ("asc".equalsIgnoreCase(colOrdering)) {
                column = column.asc();
            } else if ("desc".equalsIgnoreCase(colOrdering)) {
                column = column.desc();
            }

            columns[i] = column;
        }

        orderBy(columns);
    }

    /**
     * Sets sorting.
     *
     * @param columns columns array for sorting
     */
    public final void orderBy(ColumnMeta<?>... columns) {

        prepareOrderBy(columns);

        if (!fieldsForStatement.isEmpty()) {
            fillFieldsForStatement();
        }

        closeSet();
    }

    /**
     * Clears sorting.
     */
    public final void orderBy() {
        orderBy(new ColumnMeta<?>[0]);
    }

    private void prepareOrderBy(ColumnMeta<?>... columns) {

        ArrayList<String> l = new ArrayList<>(8);
        ArrayList<Boolean> ol = new ArrayList<>(8);
        Set<String> colNames = new HashSet<>();

        for (ColumnMeta<?> column : columns) {
            final String colName = column.getName();
            validateColumnName(colName);
            if (!colNames.add(colName)) {
                throw new CelestaException("Column '%s' is used more than once in orderby() call", colName);
            }

            l.add(String.format("\"%s\"", colName));

            final ColumnMeta.Ordering ordering = column.ordering();
            ol.add(!(ordering == null || ordering == ColumnMeta.Ordering.ASC));
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

    abstract void appendPK(List<String> l, List<Boolean> ol, final Set<String> colNames);

    /**
     * Resets filters, sorting and fully cleans the buffer.
     */
    @Override
    public void clear() {
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

    /**
     * Returns number of records in the filtered data set.
     *
     * @return
     */
    public final int count() {
        PreparedStatement stmt = count.getStatement(_currentValues(), 0);
        int result = count(stmt);
        // we are not holding this query: it's rarely used.
        count.close();
        return result;
    }

    /**
     * Method that returns total count of rows that precede the current
     * one in the set. This method is intended for internal use by GridDriver.
     * Since rows counting is a resource-consuming operation, usage of this method should
     * be avoided.
     *
     * @return
     */
    public final int position() {
        PreparedStatement stmt = position.getStatement(_currentValues(), 0);
        LOGGER.trace("{}", stmt);
        return count(stmt);
    }

    private int count(PreparedStatement stmt) {
        int result;
        try (ResultSet rs = stmt.executeQuery()) {
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
     * Gets a copy of filters along with values of limit (offset and rowcount) from
     * a cursor of the same type.
     *
     * @param c cursor the filters of which have to be copied
     */
    public final void copyFiltersFrom(BasicCursor c) {
        if (!(c._grainName().equals(_grainName()) && c._objectName().equals(_objectName()))) {
            throw new CelestaException(
                    "Cannot assign filters from cursor for %s.%s to cursor for %s.%s.",
                    c._grainName(), c._objectName(), _grainName(), _objectName());
        }
        filters.clear();
        filters.putAll(c.filters);
        complexFilter = c.complexFilter;
        copySpecificFiltersFrom(c);
        offset = c.offset;
        rowCount = c.rowCount;
        closeSet();
    }

    protected void copySpecificFiltersFrom(BasicCursor c) {
    }

    /**
     * Gets a copy of orderings from a cursor of the same type.
     *
     * @param c cursor the sortings of which have to be copied
     */
    public final void copyOrderFrom(BasicCursor c) {
        if (!(c._grainName().equals(_grainName()) && c._objectName().equals(_objectName()))) {
            throw new CelestaException(
                    "Cannot assign ordering from cursor for %s.%s to cursor for %s.%s.",
                    c._grainName(), c._objectName(), _grainName(), _objectName());
        }
        orderByNames = c.orderByNames;
        orderByIndices = c.orderByIndices;
        descOrders = c.descOrders;
        closeSet();
    }

    /**
     * Checks if filters and sorting are equivalent for this and other cursor.
     *
     * @param c Other cursor.
     * @return
     */
    public boolean isEquivalent(BasicCursor c) {
        // equality of all simple filters
        if (filters.size() != c.filters.size()) {
            return false;
        }
        for (Map.Entry<String, AbstractFilter> e : filters.entrySet()) {
            if (!e.getValue().filterEquals(c.filters.get(e.getKey()))) {
                return false;
            }
        }

        // equality of complex filters
        if (complexFilter != null && c.complexFilter != null) {
            if (!complexFilter.getCSQL().equals(c.complexFilter.getCSQL())) {
                return false;
            }
        } else {
            //one of them is null
            if (complexFilter != c.complexFilter) {
                return false;
            }
        }

        // equality of In filter
        if (!isEquivalentSpecific(c)) {
            return false;
        }
        // equality of sorting
        if (orderByNames == null) {
            orderBy();
        }
        if (c.orderByNames == null) {
            c.orderBy();
        }

        if (!Arrays.equals(orderByNames, c.orderByNames)) {
            return false;
        }

        return Arrays.equals(descOrders, c.descOrders);
    }


    boolean isEquivalentSpecific(BasicCursor c) {
        return true;
    }

    /**
     * Sets value of a field by its name. This is needed for an indirect filling
     * of the cursor with data from Java (in Python, naturally, there is <code>setattr(...)</code>
     * procedure for this goal).
     *
     * @param name  field name
     * @param value field value
     */
    public final void setValue(String name, Object value) {
        validateColumnName(name);
        _setFieldValue(name, value);
    }

    /**
     * Returns a value of a field by its name. This is needed for accessing data when using generic cursors,
     * such as {@link Cursor}.
     *
     * @param name field name
     * @return
     */
    public final Object getValue(String name) {
        validateColumnName(name);
        return _getFieldValue(name);
    }

    protected final boolean inRec(String field) {
        return fieldsForStatement.isEmpty() || fieldsForStatement.contains(field);
    }

    protected In getIn() {
        return null;
    }

    //TODO:Must be refactored by new util class FromClauseGenerator
    protected FromClause getFrom() {
        FromClause result = new FromClause();
        DataGrainElement ge = meta();

        result.setGe(ge);
        result.setExpression(db().tableString(ge.getGrain().getName(), ge.getName()));

        return result;
    }

    private void fillFieldsForStatement() {
        fieldsForStatement.clear();
        fieldsForStatement = Arrays.stream(orderByColumnNames())
                .map(f -> f.replaceAll("\"", "")).collect(Collectors.toSet());
        fieldsForStatement.addAll(fields);
    }

    /**
     * Copy field values from a cursor of the same type.
     *
     * @param from cursor that field values have to be copied from
     */
    public abstract void copyFieldsFrom(BasicCursor from);

    /**
     * Returns an array of field values.
     *
     * @return
     */
    public final Object[] getCurrentValues() {
        return _currentValues();
    }

    /**
     * Clears current cursor buffer (sets all fields to null)
     *
     * @param withKeys if true, all fields will be cleared, otherwise,
     *                 primary key fields will remain unchanged.
     */
    public final void clearBuffer(boolean withKeys) {
        _clearBuffer(withKeys);
    }

    /**
     * Clones the current cursor.
     *
     * @param context call context
     * @param fetchedFields  list of fields to be fetched
     */
    public final BasicCursor getBufferCopy(CallContext context, List<String> fetchedFields) {
        return _getBufferCopy(context, fetchedFields);
    }

    // CHECKSTYLE:OFF
    /*
     * This group of methods is named according to Python rules, and not Java.
     * In Python names of protected methods are started with an underscore symbol.
     * When using methods without an underscore symbol conflicts with attribute names
     * can be caused.
     */
    protected abstract BasicCursor _getBufferCopy(CallContext context, List<String> fields);

    protected abstract Object[] _currentValues();

    protected abstract void _clearBuffer(boolean withKeys);

    protected abstract void _setFieldValue(String name, Object value);

    protected abstract Object _getFieldValue(String name);

    protected abstract void _parseResult(ResultSet rs) throws SQLException;

    // CHECKSTYLE:ON

}
