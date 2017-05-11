package ru.curs.celesta.dbutils.term;


import ru.curs.celesta.dbutils.filter.SingleValue;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * 'Setrange' filter term with a single value.
 */
public final class SingleValueTerm extends WhereTerm {
	// unquoted column name
	private final String fieldName;
	private final SingleValue filter;

	public SingleValueTerm(String fieldName, SingleValue filter) {
		this.fieldName = fieldName;
		this.filter = filter;
	}

	@Override
	public String getWhere() {
		return String.format("(\"%s\" = ?)", fieldName);
	}

	@Override
	public void programParams(List<ParameterSetter> program) {
		program.add(ParameterSetter.create(filter));
	}
}
