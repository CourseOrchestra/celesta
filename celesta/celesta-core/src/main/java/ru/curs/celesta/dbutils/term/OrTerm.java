package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * Disjunction of two where clauses.
 */
public final class OrTerm extends WhereTerm {
	private final WhereTerm l;
	private final WhereTerm r;

	private OrTerm(WhereTerm l, WhereTerm r) {
		this.l = l;
		this.r = r;
	};

	static WhereTerm construct(WhereTerm l, WhereTerm r) {
		if (l instanceof AlwaysTrue || r instanceof AlwaysTrue) {
			return AlwaysTrue.TRUE;
		} else if (l instanceof AlwaysFalse) {
			return r;
		} else if (r instanceof AlwaysFalse) {
			return l;
		} else {
			return new OrTerm(l, r);
		}
	}

	@Override
	public String getWhere() throws CelestaException {
		String ls = (l instanceof OrTerm) ? ((OrTerm) l).getOpenWhere() : l.getWhere();
		String rs = (r instanceof OrTerm) ? ((OrTerm) r).getOpenWhere() : r.getWhere();
		return String.format("(%s or %s)", ls, rs);
	}

	private String getOpenWhere() throws CelestaException {
		return String.format("%s or %s", l.getWhere(), r.getWhere());
	}

	@Override
	public void programParams(List<ParameterSetter> program) throws CelestaException {
		l.programParams(program);
		r.programParams(program);
	}
}
