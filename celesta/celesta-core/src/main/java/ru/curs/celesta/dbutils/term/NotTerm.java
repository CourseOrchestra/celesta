package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * Negation of where clause.
 */
public final class NotTerm extends WhereTerm {
	private final WhereTerm a;

	private NotTerm(WhereTerm a) {
		this.a = a;
	}

	static WhereTerm construct(WhereTerm a) {
		if (a instanceof AlwaysFalse) {
			return AlwaysTrue.TRUE;
		} else if (a instanceof AlwaysTrue) {
			return AlwaysFalse.FALSE;
		} else {
			return new NotTerm(a);
		}
	}

	@Override
	public String getWhere() throws CelestaException {
		return String.format("(not %s)", a.getWhere());
	}

	@Override
	public void programParams(List<ParameterSetter> program) throws CelestaException {
		a.programParams(program);
	}
}
