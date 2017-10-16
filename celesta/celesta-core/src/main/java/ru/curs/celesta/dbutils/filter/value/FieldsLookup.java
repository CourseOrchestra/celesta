package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ioann on 01.06.2017.
 */
public final class FieldsLookup {

	final private Table filtered;
	final private Table filtering;

	final private List<String> fields = new ArrayList<>();
	final private List<String> otherFields = new ArrayList<>();

  private Cursor cursor;
  private Cursor otherCursor;
  private Runnable lookupChangeCallback;
  private Function<FieldsLookup, Void> newLookupCallback;

  public FieldsLookup(Cursor cursor, Cursor otherCursor,
                      Runnable lookupChangeCallback,
                      Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
    if (cursor == null) {
      throw new IllegalArgumentException("Argument 'cursor' can't be null");
    }
    if (otherCursor == null) {
      throw new IllegalArgumentException("Argument 'otherCursor' can't be null");
    }

    if (cursor.callContext() != otherCursor.callContext()) {
      throw new CelestaException("CallContexts are not matching");
    }

    this.filtered = cursor.meta();
    this.cursor = cursor;
    this.filtering = otherCursor.meta();
    this.otherCursor = otherCursor;
    this.lookupChangeCallback = lookupChangeCallback;
    this.newLookupCallback = newLookupCallback;
    lookupChangeCallback.run();
  }

  public FieldsLookup(
      Table table, Table otherTable,
      Runnable lookupChangeCallback,
      Function<FieldsLookup, Void> newLookupCallback) throws CelestaException {
    this.filtered = table;
    this.filtering = otherTable;
    this.lookupChangeCallback = lookupChangeCallback;
    this.newLookupCallback = newLookupCallback;
    lookupChangeCallback.run();
  }

  public FieldsLookup and(Cursor otherCursor) throws CelestaException {
    FieldsLookup fieldsLookup = new FieldsLookup(cursor, otherCursor, lookupChangeCallback, newLookupCallback);
    newLookupCallback.apply(fieldsLookup);
    return fieldsLookup;
  }

  public FieldsLookup and(Table filtering) throws CelestaException {
    FieldsLookup fieldsLookup = new FieldsLookup(filtered, filtering, lookupChangeCallback, newLookupCallback);
    newLookupCallback.apply(fieldsLookup);
    return fieldsLookup;
  }

	public FieldsLookup add(String field, String otherField) throws CelestaException, ParseException {
		final Column column;
		final Column otherColumn;

		column = filtered.getColumn(field);
		otherColumn = filtering.getColumn(otherField);

		if (!column.getCelestaType().equals(otherColumn.getCelestaType())) {
			throw new CelestaException("Column type of %s.%s.%s is not equal to column type of %s.%s.%s",
					filtered.getGrain().getName(), filtered.getName(), field, filtering.getGrain().getName(),
					filtering.getName(), otherField);
		}

		List<String> fieldsToValidate = new ArrayList<>(fields);
		fieldsToValidate.add(field);
		Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fieldsToValidate, filtered);

		List<String> otherFieldsToValidate = new ArrayList<>(otherFields);
		otherFieldsToValidate.add(otherField);
		Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFieldsToValidate,
				filtering);

		columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

		if (columnOrdersInIndicesSet.isEmpty()) {
			throw new CelestaException(
					"There is no indices with the same order of column(s) (\"%s\") from table table %s.%s and (\"%s\") from table table %s.%s",
					String.join(",", new HashSet<>(fieldsToValidate)), filtered.getGrain().getName(),
					filtered.getName(), String.join(",", new HashSet<>(otherFieldsToValidate)),
					filtering.getGrain().getName(), filtering.getName());
		}

    fields.add(field);
    otherFields.add(otherField);

    validate();
    lookupChangeCallback.run();

    return this;
  }

	public void validate() throws CelestaException {
    Set<List<Integer>> columnOrdersInIndicesSet = getColumnOrdersInIndicesSet(fields, filtered);
    Set<List<Integer>> otherColumnOrdersInIndicesSet = getColumnOrdersInIndicesSet(otherFields, filtering);

    columnOrdersInIndicesSet.retainAll(otherColumnOrdersInIndicesSet);

    columnOrdersInIndicesSet.stream().forEach(Collections::sort);
    Optional<List<Integer>> match = columnOrdersInIndicesSet.stream()
        .filter(l -> l.equals(IntStream.range(0, l.size()).boxed().collect(Collectors.toList()))).findAny();

    match.orElseThrow(()-> new CelestaException(
            "'In' filter validation failed. Fields matched for the filter for tables %s.%s and %s.%s " +
                    "are not covered by pks or indices on these tables.",
                filtered.getGrain().getName(), filtered.getName(), filtering.getGrain().getName(), filtering.getName()));
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

	public Table getTable() {
		return filtered;
	}

	public Table getOtherTable() {
		return filtering;
	}

	public List<String> getFields() {
		return new ArrayList<>(fields);
	}

	public List<String> getOtherFields() {
		return new ArrayList<>(otherFields);
	}

	public Cursor getOtherCursor() {
		return otherCursor;
	}
}
