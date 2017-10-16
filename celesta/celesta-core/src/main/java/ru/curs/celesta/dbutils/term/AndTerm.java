package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * Conjunction of two where clauses.
 */
public final class AndTerm extends WhereTerm {
	private final WhereTerm l;
	private final WhereTerm r;

	private AndTerm(WhereTerm l, WhereTerm r) {
		this.l = l;
		this.r = r;
	};

	static WhereTerm construct(WhereTerm l, WhereTerm r) {
		if (l instanceof AlwaysFalse || r instanceof AlwaysFalse) {
			return AlwaysFalse.FALSE;
		} else if (l instanceof AlwaysTrue) {
			return r;
		} else if (r instanceof AlwaysTrue) {
			return l;
		} else {
			return new AndTerm(l, r);
		}
	}

	@Override
	public String getWhere() throws CelestaException {
		String ls = (l instanceof AndTerm) ? ((AndTerm) l).getOpenWhere() : l.getWhere();
		String rs = (r instanceof AndTerm) ? ((AndTerm) r).getOpenWhere() : r.getWhere();
		return String.format("(%s and %s)", ls, rs);
	}

	private String getOpenWhere() throws CelestaException {
		return String.format("%s and %s", l.getWhere(), r.getWhere());
	}

	@Override
	public void programParams(List<ParameterSetter> program) throws CelestaException {
		l.programParams(program);
		r.programParams(program);
	}
}
