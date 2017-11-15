package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * Comparision of a field with null.
 */
public final class IsNull extends WhereTerm {

	// quoted column name
	private final String fieldName;

	public IsNull(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	public String getWhere() {
		return String.format("(%s is null)", fieldName);
	}

	@Override
	public void programParams(List<ParameterSetter> program) {
		// do nothing - no parameters
	}

}
