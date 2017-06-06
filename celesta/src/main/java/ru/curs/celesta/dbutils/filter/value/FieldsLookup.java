package ru.curs.celesta.dbutils.filter.value;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;

import java.util.*;

/**
 * Created by ioann on 01.06.2017.
 */
public final class FieldsLookup {

	final private Table cursor;
	final private Table otherCursor;

	final private List<String> fields = new ArrayList<>();
	final private List<String> otherFields = new ArrayList<>();

	public FieldsLookup(Cursor cursor, Cursor otherCursor) throws CelestaException {
		this.cursor = cursor.meta();
		this.otherCursor = otherCursor.meta();
	}
	
	public FieldsLookup(Table table, Table otherTable) throws CelestaException {
		this.cursor = cursor.meta();
		this.otherCursor = otherCursor.meta();
	}


	public FieldsLookup add(String field, String otherField) throws CelestaException, ParseException {
		final String tableTemplate;

		tableTemplate = DBAdaptor.getAdaptor().tableTemplate();

		final Table table;
		final Column column;
		final Table otherTable;
		final Column otherColumn;

		table = cursor;
		column = table.getColumn(field);
		otherTable = otherCursor;
		otherColumn = otherTable.getColumn(otherField);

		if (!column.getCelestaType().equals(otherColumn.getCelestaType())) {
			throw new CelestaException(
					"Column type of " + tableTemplate + ".\"%s\" is not equal " + "to column type of " + tableTemplate
							+ ".\"%s\"",
					table.getGrain().getName(), table.getName(), field, otherTable.getGrain().getName(),
					otherTable.getName(), otherField);
		}

		List<String> fieldsToValidate = new ArrayList<>(fields);
		fieldsToValidate.add(field);
		validateIndices(table, fieldsToValidate, true);

		fieldsToValidate = new ArrayList<>(otherFields);
		fieldsToValidate.add(otherField);
		validateIndices(otherTable, fieldsToValidate, true);

		fields.add(field);
		otherFields.add(otherField);
		return this;
	}

	private void validateIndices(Table table, List<String> fieldList, boolean preValidation) throws CelestaException {
		Set<String> fieldSet = new HashSet<>(fieldList);

		Set<Index> indexes = table.getIndices();
		Optional<Index> index = indexes.stream().filter(i -> {
			if (preValidation) {
				return i.getColumns().keySet().containsAll(fieldSet);
			} else {
				return i.getColumns().keySet().equals(fieldSet);
			}
		}).findFirst();

		if (!index.isPresent()) {
			String tableTemplate = DBAdaptor.getAdaptor().tableTemplate();
			throw new CelestaException("There is no index for column(s) (\"%s\") in table " + tableTemplate,
					String.join(",", fieldSet), table.getGrain().getName(), table.getName());
		}
	}

	public void validate() throws CelestaException {
		validateIndices(cursor.meta(), fields, false);
		validateIndices(otherCursor.meta(), otherFields, false);
	}

	public Cursor getCursor() {
		return cursor;
	}

	public Cursor getOtherCursor() {
		return otherCursor;
	}

	public List<String> getFields() {
		return new ArrayList<>(fields);
	}

	public List<String> getOtherFields() {
		return new ArrayList<>(otherFields);
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (!(o instanceof FieldsLookup))
			return false;

		FieldsLookup other = (FieldsLookup) o;

		return Objects.equals(this.cursor.getClass().getName(), other.cursor.getClass().getName())
				&& Objects.equals(this.otherCursor.getClass().getName(), other.otherCursor.getClass().getName())
				&& this.fields.equals(other.fields) && this.otherCursor.equals(other.fields);
	}

	@Override
	public int hashCode() {
		int result = 17;

		result += 31 * cursor.getClass().getName().hashCode();
		result += 31 * otherCursor.getClass().getName().hashCode();

		return result;
	}
}
