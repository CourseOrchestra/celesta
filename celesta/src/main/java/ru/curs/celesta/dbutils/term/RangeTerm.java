package ru.curs.celesta.dbutils.term;


import ru.curs.celesta.dbutils.filter.Range;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * 'Setrange' filter term with 'from.. to' values.
 */
public final class RangeTerm extends WhereTerm {
	// unquoted column name
	private final String fieldName;
	private final Range filter;

	public RangeTerm(String fieldName, Range filter) {
		this.fieldName = fieldName;
		this.filter = filter;
	}

	@Override
	public String getWhere() {
		return String.format("(\"%s\" between ? and ?)", fieldName);
	}

	@Override
	public void programParams(List<ParameterSetter> program) {
		program.add(ParameterSetter.createForValueFrom(filter));
		program.add(ParameterSetter.createForValueTo(filter));
	}
}
