package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * Comparision of a field with a value.
 */
public final class FieldCompTerm extends WhereTerm {
	// quoted column name
	private final String fieldName;
	private final int fieldIndex;
	private final String op;

	public FieldCompTerm(String fieldName, int fieldIndex, String op) {
		this.fieldName = fieldName;
		this.fieldIndex = fieldIndex;
		this.op = op;
	}

	@Override
	public String getWhere() {
		return String.format("(%s %s ?)", fieldName, op);
	}

	@Override
	public void programParams(List<ParameterSetter> program) {
		program.add(ParameterSetter.create(fieldIndex));
	}
}
