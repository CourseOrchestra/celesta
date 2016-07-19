package ru.curs.celesta.dbutils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Expr;

/**
 * The interface that provides needed information for building filter/navigation
 * queries. This could be Cursor, but we extracted this interface for
 * testability.
 */
interface WhereMakerParamsProvider {
	QueryBuildingHelper dba();

	void initOrderBy() throws CelestaException;

	String[] sortFields() throws CelestaException;

	int[] sortFieldsIndices() throws CelestaException;

	boolean[] descOrders() throws CelestaException;

	Object[] values() throws CelestaException;

	boolean isNullable(String columnName) throws CelestaException;

	Map<String, AbstractFilter> filters();

	Expr complexFilter();
}

/**
 * A term of filter/navigation where clause.
 */
abstract class WhereTerm {
	abstract String getWhere() throws CelestaException;

	abstract void programParams(List<ParameterSetter> program) throws CelestaException;
}

/**
 * TRUE constant.
 */
final class AlwaysTrue extends WhereTerm {

	public static final AlwaysTrue TRUE = new AlwaysTrue();

	private AlwaysTrue() {
	};

	@Override
	String getWhere() {
		return "(1 = 1)";
	}

	@Override
	void programParams(List<ParameterSetter> program) {
		// do nothing, no parameters
	}
}

/**
 * FALSE constant.
 */
final class AlwaysFalse extends WhereTerm {

	public static final AlwaysFalse FALSE = new AlwaysFalse();

	private AlwaysFalse() {
	};

	@Override
	String getWhere() {
		return "(1 = 0)";
	}

	@Override
	void programParams(List<ParameterSetter> program) {
		// do nothing, no parameters
	}
}

/**
 * Negation of where clause.
 */
final class NotTerm extends WhereTerm {
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
	String getWhere() throws CelestaException {
		return String.format("(not %s)", a.getWhere());
	}

	@Override
	void programParams(List<ParameterSetter> program) throws CelestaException {
		a.programParams(program);
	}
}

/**
 * Conjunction of two where clauses.
 */
final class AndTerm extends WhereTerm {
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
	String getWhere() throws CelestaException {
		return String.format("(%s and %s)", l.getWhere(), r.getWhere());
	}

	@Override
	void programParams(List<ParameterSetter> program) throws CelestaException {
		l.programParams(program);
		r.programParams(program);
	}
}

/**
 * Disjunction of two where clauses.
 */
final class OrTerm extends WhereTerm {
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
	String getWhere() throws CelestaException {
		return String.format("(%s or %s)", l.getWhere(), r.getWhere());
	}

	@Override
	void programParams(List<ParameterSetter> program) throws CelestaException {
		l.programParams(program);
		r.programParams(program);
	}
}

/**
 * 'Setrange' filter term with a single value.
 */
final class SingleValueTerm extends WhereTerm {
	// unquoted column name
	private final String fieldName;
	private final SingleValue filter;

	public SingleValueTerm(String fieldName, SingleValue filter) {
		this.fieldName = fieldName;
		this.filter = filter;
	}

	@Override
	String getWhere() {
		return String.format("(\"%s\" = ?)", fieldName);
	}

	@Override
	void programParams(List<ParameterSetter> program) {
		program.add(ParameterSetter.create(filter));
	}
}

/**
 * 'Setrange' filter term with 'from.. to' values.
 */
final class RangeTerm extends WhereTerm {
	// unquoted column name
	private final String fieldName;
	private final Range filter;

	public RangeTerm(String fieldName, Range filter) {
		this.fieldName = fieldName;
		this.filter = filter;
	}

	@Override
	String getWhere() {
		return String.format("(\"%s\" between ? and ?)", fieldName);
	}

	@Override
	void programParams(List<ParameterSetter> program) {
		program.add(ParameterSetter.createForValueFrom(filter));
		program.add(ParameterSetter.createForValueTo(filter));
	}
}

/**
 * Comparision of a field with null.
 */
final class IsNull extends WhereTerm {

	// quoted column name
	private final String fieldName;

	public IsNull(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	String getWhere() {
		return String.format("(%s is null)", fieldName);
	}

	@Override
	void programParams(List<ParameterSetter> program) {
		// do nothing - no parameters
	}

}

/**
 * Produces navigation queries.
 */
class WhereTermsMaker {
	/**
	 * Term factory constructor.
	 */
	@FunctionalInterface
	private interface TermConstructor {
		WhereTerm create(String fieldName, int fieldIndex, WhereTermsMaker m);
	}

	static final int GT = 0; // >
	static final int GE = 1; // >=
	static final int EQ = 2; // =
	static final int LE = 3; // <=
	static final int LT = 4; // <

	// COMPARISION TABLE TERM FACTORY
	private static final TermConstructor[] C;

	static {
		// CHECKSTYLE:OFF this is meta-programming
		C = new TermConstructor[40];
		// NOT NULL, NF, REGULAR
		C[0] = (f, i, m) -> {
			return m.new CompTerm(f, i, ">");
		};
		C[1] = (f, i, m) -> {
			return m.new CompTerm(f, i, ">=");
		};
		C[2] = (f, i, m) -> {
			return m.new CompTerm(f, i, "=");
		};
		C[3] = (f, i, m) -> {
			return m.new CompTerm(f, i, "<=");
		};
		C[4] = (f, i, m) -> {
			return m.new CompTerm(f, i, "<");
		};

		// NOT NULL, NF, NULL
		C[5] = (f, i, m) -> {
			return AlwaysTrue.TRUE;
		};
		C[6] = C[5];
		C[7] = (f, i, m) -> {
			return AlwaysFalse.FALSE;
		};
		C[8] = C[7];
		C[9] = C[7];

		// NOT NULL, NL, REGULAR
		C[10] = C[0];
		C[11] = C[1];
		C[12] = C[2];
		C[13] = C[3];
		C[14] = C[4];

		// NOT NULL, NL, NULL
		C[15] = C[7];
		C[16] = C[7];
		C[17] = C[7];
		C[18] = C[5];
		C[19] = C[5];

		// NULLABLE, NF, REGULAR
		C[20] = C[0];
		C[21] = C[1];
		C[22] = C[2];
		C[23] = (f, i, m) -> {
			return OrTerm.construct(m.new CompTerm(f, i, "<="), new IsNull(f));
		};
		C[24] = (f, i, m) -> {
			return OrTerm.construct(m.new CompTerm(f, i, "<"), new IsNull(f));
		};

		// NULLABLE, NF, NULL
		C[25] = (f, i, m) -> {
			return NotTerm.construct(new IsNull(f));
		};
		C[26] = C[5];
		C[27] = (f, i, m) -> {
			return new IsNull(f);
		};
		C[28] = C[27];
		C[29] = C[7];

		// NULLABLE, NL, REGULAR
		C[30] = (f, i, m) -> {
			return OrTerm.construct(m.new CompTerm(f, i, ">"), new IsNull(f));
		};
		C[31] = (f, i, m) -> {
			return OrTerm.construct(m.new CompTerm(f, i, ">="), new IsNull(f));
		};
		C[32] = C[2];
		C[33] = C[3];
		C[34] = C[4];

		// NULLABLE, NL, NULL
		C[35] = C[7];
		C[36] = C[27];
		C[37] = C[27];
		C[38] = C[5];
		C[39] = C[25];
		// CHECKSTYLE:ON
	}

	private final WhereMakerParamsProvider paramsProvider;

	private Object[] rec;

	WhereTermsMaker(WhereMakerParamsProvider paramsProvider) {
		this.paramsProvider = paramsProvider;
	}

	static int ind(boolean nullable, boolean nf, boolean isNull, int op) {
		return ((nullable ? 4 : 0) + (nf ? 0 : 2) + (isNull ? 1 : 0)) * 5 + op;
	}

	/**
	 * Gets WHERE clause for filtered rowset.
	 * 
	 * @throws CelestaException
	 *             Celesta error.
	 */
	public WhereTerm getWhereTerm() throws CelestaException {
		paramsProvider.initOrderBy();
		rec = paramsProvider.values();

		WhereTerm r = null;
		for (Entry<String, AbstractFilter> e : paramsProvider.filters().entrySet()) {
			final WhereTerm l;
			final AbstractFilter f = e.getValue();
			if (f instanceof SingleValue) {
				l = new SingleValueTerm(e.getKey(), (SingleValue) f);
			} else if (f instanceof Range) {
				l = new RangeTerm(e.getKey(), (Range) f);
			} else {
				l = new FilterTerm(e.getKey(), (Filter) f);
			}
			r = r == null ? l : AndTerm.construct(l, r);

		}

		final WhereTerm l = r == null ? AlwaysTrue.TRUE : r;
		r = paramsProvider.complexFilter() == null ? AlwaysTrue.TRUE : new ComplexFilterTerm();
		return AndTerm.construct(l, r);

	}

	/**
	 * Gets WHERE clause for navigational term with respect of filters and
	 * database settings.
	 * 
	 * @param op
	 *            navigation operator: '>', '<', or '='.
	 * @throws CelestaException
	 *             Invalid navigation operator.
	 */
	public WhereTerm getWhereTerm(char op) throws CelestaException {
		paramsProvider.initOrderBy();
		rec = paramsProvider.values();

		boolean invert = false;
		switch (op) {
		case '>':
			invert = false;
			break;
		case '<':
			invert = true;
			break;
		case '=':
			return AndTerm.construct(getWhereTerm(), getEqualsWhereTerm(0));
		default:
			throw new CelestaException("Invalid navigation operator: %s", op);
		}

		int l = paramsProvider.sortFields().length;
		char[] ops = new char[l];
		for (int i = 0; i < l; i++) {
			ops[i] = (invert ^ paramsProvider.descOrders()[i]) ? '<' : '>';
		}
		return AndTerm.construct(getWhereTerm(), getWhereTerm(ops, 0));
	}

	private boolean isNull(int k) throws CelestaException {
		return rec[paramsProvider.sortFieldsIndices()[k]] == null;
	}

	private WhereTerm getEqualsWhereTerm(int k) throws CelestaException {
		final String fieldName = paramsProvider.sortFields()[k];
		final int fieldIndex = paramsProvider.sortFieldsIndices()[k];
		final boolean nullable = treatAsNullable(fieldName);

		WhereTerm l = C[ind(nullable, paramsProvider.dba().nullsFirst(), isNull(k), EQ)].create(fieldName, fieldIndex,
				this);
		if (paramsProvider.sortFields().length - 1 > k) {
			WhereTerm r = getEqualsWhereTerm(k + 1);
			return AndTerm.construct(l, r);
		} else {
			return l;
		}
	}

	private WhereTerm getWhereTerm(char[] ops, int k) throws CelestaException {
		final String fieldName = paramsProvider.sortFields()[k];
		final int fieldIndex = paramsProvider.sortFieldsIndices()[k];
		final boolean isNull = isNull(k);
		final boolean nf = paramsProvider.dba().nullsFirst();
		final boolean nullable = treatAsNullable(fieldName);

		if (paramsProvider.sortFields().length - 1 > k) {
			WhereTerm a = C[ind(nullable, nf, isNull, ops[k] == '>' ? GE : LE)].create(fieldName, fieldIndex, this);
			WhereTerm b = C[ind(nullable, nf, isNull, ops[k] == '>' ? GT : LT)].create(fieldName, fieldIndex, this);
			WhereTerm c = getWhereTerm(ops, k + 1);
			return AndTerm.construct(a, OrTerm.construct(b, c));
		} else {
			return C[ind(nullable, nf, isNull, ops[k] == '>' ? GT : LT)].create(fieldName, fieldIndex, this);
		}
	}

	private boolean treatAsNullable(String fieldName) throws CelestaException {
		// if a Range filter is set on the field, we treat it as NOT NULL
		// (no nulls will be in the record set anyway).
		String name = unquot(fieldName);
		if (paramsProvider.isNullable(name)) {
			final AbstractFilter f = paramsProvider.filters().get(name);
			return !(f instanceof SingleValue || f instanceof Range);
		} else {
			return false;
		}
	}

	static String unquot(String name) {
		return name.substring(1, name.length() - 1);
	}

	/**
	 * 'SetFilter' filter term.
	 */
	final class FilterTerm extends WhereTerm {
		// unquoted column name
		private final String fieldName;
		private final Filter filter;

		public FilterTerm(String fieldName, Filter filter) {
			this.fieldName = fieldName;
			this.filter = filter;
		}

		@Override
		String getWhere() throws CelestaException {
			return "(" + filter.makeWhereClause("\"" + fieldName + "\"", paramsProvider.dba()) + ")";
		}

		@Override
		void programParams(List<ParameterSetter> program) {
			// do nothing - no parameters
		}

	}

	/**
	 * Comparision of a field with a value.
	 */
	final class CompTerm extends WhereTerm {
		// quoted column name
		private final String fieldName;
		private final int fieldIndex;
		private final String op;

		public CompTerm(String fieldName, int fieldIndex, String op) {
			this.fieldName = fieldName;
			this.fieldIndex = fieldIndex;
			this.op = op;
		}

		@Override
		String getWhere() {
			return String.format("(%s %s ?)", fieldName, op);
		}

		@Override
		void programParams(List<ParameterSetter> program) {
			program.add(ParameterSetter.create(fieldIndex));
		}
	}

	/**
	 * A term for a complex (CelestaSQL) filter.
	 */
	final class ComplexFilterTerm extends WhereTerm {

		@Override
		String getWhere() throws CelestaException {
			return paramsProvider.complexFilter().getSQL(paramsProvider.dba());
		}

		@Override
		void programParams(List<ParameterSetter> program) {
			// do nothing - no parameters
		}

	}

}
