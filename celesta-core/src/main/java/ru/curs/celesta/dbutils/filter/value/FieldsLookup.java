package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.ViewCursor;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ioann on 01.06.2017.
 */
public final class FieldsLookup {

    private final Class<? extends DataGrainElement> targetClass;
    private final DataGrainElement filtered;
    private final DataGrainElement filtering;

    private final List<String> fields = new ArrayList<>();
    private final List<String> otherFields = new ArrayList<>();

    private BasicCursor cursor;
    private BasicCursor otherCursor;
    private Runnable lookupChangeCallback;
    private Function<FieldsLookup, Void> newLookupCallback;

    public FieldsLookup(Cursor cursor, BasicCursor otherCursor,
                        Runnable lookupChangeCallback,
                        Function<FieldsLookup, Void> newLookupCallback) {
        this(BasicTable.class, cursor, cursor.meta(), otherCursor,
                otherCursor.meta(), lookupChangeCallback, newLookupCallback);
    }

    public FieldsLookup(ViewCursor cursor, BasicCursor otherCursor,
                        Runnable lookupChangeCallback,
                        Function<FieldsLookup, Void> newLookupCallback) {
        this(View.class, cursor, cursor.meta(), otherCursor,
                otherCursor.meta(), lookupChangeCallback, newLookupCallback);
    }

    private FieldsLookup(Class<? extends DataGrainElement> targetClass, BasicCursor cursor,
                         DataGrainElement filtered, BasicCursor otherCursor,
                         DataGrainElement filtering, Runnable lookupChangeCallback,
                         Function<FieldsLookup, Void> newLookupCallback) {
        validateCursors(cursor, otherCursor);
        this.cursor = cursor;
        this.otherCursor = otherCursor;
        this.filtered = filtered;
        this.filtering = filtering;
        this.targetClass = targetClass;

        this.lookupChangeCallback = lookupChangeCallback;
        this.newLookupCallback = newLookupCallback;
        lookupChangeCallback.run();
    }

    /**
     * This constructor is for tests and must be removed in future.
     *
     * @param table
     * @param otherTable
     * @param lookupChangeCallback
     * @param newLookupCallback
     */
    public FieldsLookup(
            BasicTable table, BasicTable otherTable,
            Runnable lookupChangeCallback,
            Function<FieldsLookup, Void> newLookupCallback) {
        this.targetClass = BasicTable.class;
        this.filtered = table;
        this.filtering = otherTable;
        this.lookupChangeCallback = lookupChangeCallback;
        this.newLookupCallback = newLookupCallback;
        lookupChangeCallback.run();
    }

    /**
     * This constructor is for tests and must be removed in future.
     *
     * @param view
     * @param otherView
     * @param lookupChangeCallback
     * @param newLookupCallback
     */
    public FieldsLookup(
            View view, View otherView,
            Runnable lookupChangeCallback,
            Function<FieldsLookup, Void> newLookupCallback) {
        this.targetClass = View.class;
        this.filtered = view;
        this.filtering = otherView;
        this.lookupChangeCallback = lookupChangeCallback;
        this.newLookupCallback = newLookupCallback;
        lookupChangeCallback.run();
    }

    private void validateCursors(BasicCursor cursor, BasicCursor otherCursor) {
        if (cursor == null) {
            throw new IllegalArgumentException("Argument 'cursor' can't be null");
        }
        if (otherCursor == null) {
            throw new IllegalArgumentException("Argument 'otherCursor' can't be null");
        }

        if (cursor.callContext() != otherCursor.callContext()) {
            throw new CelestaException("CallContexts are not matching");
        }
    }

    public FieldsLookup and(BasicCursor otherCursor) {
        if (BasicTable.class.equals(targetClass)) {
            FieldsLookup fieldsLookup = new FieldsLookup(
                    (Cursor) cursor, otherCursor, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else if (View.class.equals(targetClass)) {
            FieldsLookup fieldsLookup = new FieldsLookup(
                    (ViewCursor) cursor, otherCursor, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else {
            throw new CelestaException("Can't apply %s to %s for FieldsLookup",
                    otherCursor.meta().getClass().getSimpleName(), filtered.getClass().getSimpleName());
        }
    }

    public FieldsLookup and(BasicTable filtering) {

        if (filtered instanceof BasicTable) {
            BasicTable filteredTable = (BasicTable) filtered;
            FieldsLookup fieldsLookup = new FieldsLookup(
                    filteredTable, filtering, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else {
            throw new CelestaException("Can't apply Table to %s for FieldsLookup", filtered.getClass().getSimpleName());
        }
    }

    public FieldsLookup and(View filtering) {

        if (filtered instanceof View) {
            View filteredView = (View) filtered;
            FieldsLookup fieldsLookup = new FieldsLookup(
                    filteredView, filtering, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else {
            throw new CelestaException("Can't apply Table to %s for FieldsLookup", filtered.getClass().getSimpleName());
        }
    }

    /**
     * Adds fields binding for target and auxiliary cursors.
     *
     * @param field      filed of the target cursor.
     * @param otherField field of the auxiliary cursor.
     * @return
     * @throws ParseException if some column is not found.
     */
    @Deprecated
    public FieldsLookup add(String field, String otherField) throws ParseException {
        return internalAdd(validateFilteredColumn(field), validateFilteringColumn(otherField));
    }

    /**
     * Adds columns binding for target and auxiliary cursors.
     *
     * @param column      column of the target cursor.
     * @param otherColumn column of the auxiliary cursor.
     * @param <T>         type of the column. Only columns of the same type can be bound in one filter.
     * @return
     * @throws ParseException if a column is not found in the relevant table.
     */
    public <T> FieldsLookup add(ColumnMeta<T> column, ColumnMeta<T> otherColumn) throws ParseException {
        return internalAdd(column, otherColumn);
    }

    private FieldsLookup internalAdd(final ColumnMeta<?> column, final ColumnMeta<?> otherColumn) throws ParseException {

        final String columnName = column.getName();
        final String otherColumnName = otherColumn.getName();

        validateFilteredColumn(columnName);
        validateFilteringColumn(otherColumnName);

        if (!column.getCelestaType().equals(otherColumn.getCelestaType())) {
            throw new CelestaException("Column type of %s.%s.%s is not equal to column type of %s.%s.%s",
                    filtered.getGrain().getName(), filtered.getName(), columnName,
                    filtering.getGrain().getName(), filtering.getName(), otherColumnName);
        }

        if (filtered instanceof Table && filtering instanceof Table) {
            List<String> fieldsToValidate = new ArrayList<>(fields);
            fieldsToValidate.add(columnName);
            Set<List<Integer>> columnOrdersInIndicesSet =
                    getColumnOrdersInIndicesSet(fieldsToValidate, (BasicTable) filtered);

            List<String> otherFieldsToValidate = new ArrayList<>(otherFields);
            otherFieldsToValidate.add(otherColumnName);
            Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFieldsToValidate,
                    (BasicTable) filtering);

            columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

            if (columnOrdersInIndicesSet.isEmpty()) {
                throw new CelestaException(
                        "There is no indices with the same order of column(s) (\"%s\") from table"
                                + " table %s.%s and (\"%s\") from table table %s.%s",
                        String.join(",", new HashSet<>(fieldsToValidate)), filtered.getGrain().getName(),
                        filtered.getName(), String.join(",", new HashSet<>(otherFieldsToValidate)),
                        filtering.getGrain().getName(), filtering.getName());
            }
        }

        fields.add(columnName);
        otherFields.add(otherColumnName);

        validate();
        lookupChangeCallback.run();

        return this;
    }

    private ColumnMeta<?> validateFilteredColumn(String columnField) throws ParseException {
        final ColumnMeta<?> column = filtered.getColumns().get(columnField);
        if (column == null) {
            throw new ParseException(String.format("Column '%s' not found in %s '%s.%s'",
                    columnField, targetClass.getSimpleName(), filtered.getGrain().getName(), filtered.getName()));
        }

        return column;
    }

    private ColumnMeta<?> validateFilteringColumn(String columnField) throws ParseException {
        final ColumnMeta<?> column = filtering.getColumns().get(columnField);
        if (column == null) {
            throw new ParseException(String.format("Column '%s' not found in %s '%s.%s'",
                    columnField, targetClass.getSimpleName(), filtering.getGrain().getName(), filtering.getName()));
        }

        return column;
    }

    public void validate() {
        if (filtered instanceof Table && filtering instanceof Table) {
            Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fields, (BasicTable) filtered);
            Set<List<Integer>> otherColumnOrdersInIndicesSet =
                    getColumnOrdersInIndicesSet(otherFields, (BasicTable) filtering);

            columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

            columnOrdersInIndicesSet.stream().forEach(Collections::sort);
            Optional<List<Integer>> match = columnOrdersInIndicesSet.stream()
                    .filter(l -> l.equals(IntStream.range(0, l.size()).boxed().collect(Collectors.toList()))).findAny();

            match.orElseThrow(() -> new CelestaException(
                    "'In' filter validation failed. Fields matched for the filter for tables %s.%s and %s.%s"
                            + " are not covered by pks or indices on these tables.",
                    filtered.getGrain().getName(), filtered.getName(),
                    filtering.getGrain().getName(), filtering.getName()));
        }
    }

    private Set<List<Integer>> getColumnOrdersInIndicesSet(List<String> fieldsToValidate, BasicTable table) {

        final List<Index> indexesToValidate;
        // Сперва определяем, есть ли указанные поля в первичном ключе
        boolean pkContainsFields = table.getPrimaryKey().keySet().containsAll(fieldsToValidate);
        //Затем выбираем все индексы, содержащие пришедшие поля
        Set<Index> indexes = table.getIndices();
        indexesToValidate = indexes.stream().filter(i -> i.getColumns().keySet().containsAll(fieldsToValidate))
                .collect(Collectors.toList());

        if (!pkContainsFields && indexesToValidate.isEmpty()) {
            throw new CelestaException("There is no pk or index which contains column(s) (\"%s\") in table %s.%s",
                    String.join(",", new HashSet<>(fieldsToValidate)), table.getGrain().getName(), table.getName());
        }

        Set<List<Integer>> result = new HashSet<>();

        if (pkContainsFields) {
            List<String> pkColNames = new ArrayList<>(table.getPrimaryKey().keySet());
            result.add(fieldsToValidate.stream().map(pkColNames::indexOf).collect(Collectors.toList()));
        }

        result.addAll(indexesToValidate.stream()
                .map(i -> fieldsToValidate.stream().map(i::getColumnIndex).collect(Collectors.toList()))
                .collect(Collectors.toSet()));

        return result;
    }

    public DataGrainElement getFiltered() {
        return filtered;
    }

    public DataGrainElement getFiltering() {
        return filtering;
    }

    public List<String> getFields() {
        return new ArrayList<>(fields);
    }

    public List<String> getOtherFields() {
        return new ArrayList<>(otherFields);
    }

    public BasicCursor getOtherCursor() {
        return otherCursor;
    }
}
