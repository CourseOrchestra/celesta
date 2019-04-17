package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.ViewCursor;
import ru.curs.celesta.score.*;

import java.util.*;
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

    final private List<String> fields = new ArrayList<>();
    final private List<String> otherFields = new ArrayList<>();

    private BasicCursor cursor;
    private BasicCursor otherCursor;
    private Runnable lookupChangeCallback;
    private Function<FieldsLookup, Void> newLookupCallback;

    public FieldsLookup(Cursor cursor, Cursor otherCursor,
                        Runnable lookupChangeCallback,
                        Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
        this(Table.class, cursor, cursor.meta(), otherCursor, otherCursor.meta(), lookupChangeCallback, newLookupCallback);
    }

    public FieldsLookup(ViewCursor cursor, ViewCursor otherCursor,
                        Runnable lookupChangeCallback,
                        Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
        this(View.class, cursor, cursor.meta(), otherCursor, otherCursor.meta(), lookupChangeCallback, newLookupCallback);
    }

    private FieldsLookup(Class<? extends DataGrainElement> targetClass, BasicCursor cursor, DataGrainElement filtered, BasicCursor otherCursor,
                         DataGrainElement filtering, Runnable lookupChangeCallback,
                         Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
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
     * This constructor is for tests and must be removed in future
     *
     * @param table
     * @param otherTable
     * @param lookupChangeCallback
     * @param newLookupCallback
     * @throws CelestaException
     */
    public FieldsLookup(
            Table table, Table otherTable,
            Runnable lookupChangeCallback,
            Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
        this.targetClass = Table.class;
        this.filtered = table;
        this.filtering = otherTable;
        this.lookupChangeCallback = lookupChangeCallback;
        this.newLookupCallback = newLookupCallback;
        lookupChangeCallback.run();
    }

    /**
     * This constructor is for tests and must be removed in future
     *
     * @param view
     * @param otherView
     * @param lookupChangeCallback
     * @param newLookupCallback
     * @throws CelestaException
     */
    public FieldsLookup(
            View view, View otherView,
            Runnable lookupChangeCallback,
            Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
        this.targetClass = View.class;
        this.filtered = view;
        this.filtering = otherView;
        this.lookupChangeCallback = lookupChangeCallback;
        this.newLookupCallback = newLookupCallback;
        lookupChangeCallback.run();
    }

    private void validateCursors(BasicCursor cursor, BasicCursor otherCursor) throws CelestaException {
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

    public FieldsLookup and(BasicCursor otherCursor) throws CelestaException {
        if (Table.class.equals(targetClass)) {
            FieldsLookup fieldsLookup = new FieldsLookup((Cursor) cursor, (Cursor) otherCursor, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else if (View.class.equals(targetClass)) {
            FieldsLookup fieldsLookup = new FieldsLookup((ViewCursor) cursor, (ViewCursor) otherCursor, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else {
            throw new CelestaException("Can't apply %s to %s for FieldsLookup", otherCursor.meta().getClass().getSimpleName(), filtered.getClass().getSimpleName());
        }
    }

    public FieldsLookup and(Table filtering) throws CelestaException {

        if (filtered instanceof Table) {
            Table filteredTable = (Table) filtered;
            FieldsLookup fieldsLookup = new FieldsLookup(filteredTable, filtering, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else {
            throw new CelestaException("Can't apply Table to %s for FieldsLookup", filtered.getClass().getSimpleName());
        }
    }

    public FieldsLookup and(View filtering) throws CelestaException {

        if (filtered instanceof View) {
            View filteredView = (View) filtered;
            FieldsLookup fieldsLookup = new FieldsLookup(filteredView, filtering, lookupChangeCallback, newLookupCallback);
            newLookupCallback.apply(fieldsLookup);
            return fieldsLookup;
        } else {
            throw new CelestaException("Can't apply Table to %s for FieldsLookup", filtered.getClass().getSimpleName());
        }
    }

    public FieldsLookup add(String field, String otherField) throws CelestaException, ParseException {
        final ColumnMeta column;
        final ColumnMeta otherColumn;

        column = filtered.getColumns().get(field);
        otherColumn = filtering.getColumns().get(otherField);

        if (column == null) {
                throw new ParseException(
                        String.format("Column '%s' not found in %s '%s.%s'",
                                field, targetClass.getSimpleName(), filtered.getGrain().getName(), filtered.getName()));
        }
        if (otherColumn == null) {
            throw new ParseException(
                    String.format("Column '%s' not found in %s '%s.%s'",
                            otherField, targetClass.getSimpleName(), filtering.getGrain().getName(), filtering.getName()));
        }


        if (!column.getCelestaType().equals(otherColumn.getCelestaType())) {
            throw new CelestaException("Column type of %s.%s.%s is not equal to column type of %s.%s.%s",
                    filtered.getGrain().getName(), filtered.getName(), field,
                    filtering.getGrain().getName(), filtering.getName(), otherField);
        }

        if (Table.class.equals(targetClass)) {
            List<String> fieldsToValidate = new ArrayList<>(fields);
            fieldsToValidate.add(field);
            Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fieldsToValidate, (Table)filtered);

            List<String> otherFieldsToValidate = new ArrayList<>(otherFields);
            otherFieldsToValidate.add(otherField);
            Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFieldsToValidate,
                    (Table)filtering);

            columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

            if (columnOrdersInIndicesSet.isEmpty()) {
                throw new CelestaException(
                        "There is no indices with the same order of column(s) (\"%s\") from table table %s.%s and (\"%s\") from table table %s.%s",
                        String.join(",", new HashSet<>(fieldsToValidate)), filtered.getGrain().getName(),
                        filtered.getName(), String.join(",", new HashSet<>(otherFieldsToValidate)),
                        filtering.getGrain().getName(), filtering.getName());
            }
        }

        fields.add(field);
        otherFields.add(otherField);

        validate();
        lookupChangeCallback.run();

        return this;
    }

    public void validate() throws CelestaException {
        if (Table.class.equals(targetClass)) {
            Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fields, (Table)filtered);
            Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFields, (Table)filtering);

            columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

            columnOrdersInIndicesSet.stream().forEach(Collections::sort);
            Optional<List<Integer>> match = columnOrdersInIndicesSet.stream()
                    .filter(l -> l.equals(IntStream.range(0, l.size()).boxed().collect(Collectors.toList()))).findAny();

            match.orElseThrow(() -> new CelestaException(
                    "'In' filter validation failed. Fields matched for the filter for tables %s.%s and %s.%s " +
                            "are not covered by pks or indices on these tables.",
                    filtered.getGrain().getName(), filtered.getName(), filtering.getGrain().getName(), filtering.getName()));
        }
    }

    private Set<List<Integer>> getColumnOrdersInIndicesSet(List<String> fieldsToValidate, Table table)
            throws CelestaException {

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
            result.add(fieldsToValidate.stream().map(f -> pkColNames.indexOf(f)).collect(Collectors.toList()));
        }

        result.addAll(indexesToValidate.stream()
                .map(i -> fieldsToValidate.stream().map(f -> i.getColumnIndex(f)).collect(Collectors.toList()))
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
