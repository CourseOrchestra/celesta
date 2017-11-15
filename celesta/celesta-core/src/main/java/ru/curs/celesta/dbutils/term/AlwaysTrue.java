package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * TRUE constant.
 */
public final class AlwaysTrue extends WhereTerm {

	public static final AlwaysTrue TRUE = new AlwaysTrue();

	private AlwaysTrue() {
	};

	@Override
	public String getWhere() {
		return "(1 = 1)";
	}

	@Override
	public void programParams(List<ParameterSetter> program) {
		// do nothing, no parameters
	}
}
