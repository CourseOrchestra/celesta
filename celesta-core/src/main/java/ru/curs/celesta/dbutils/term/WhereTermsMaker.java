package ru.curs.celesta.dbutils.term;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.filter.*;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.score.BasicTable;

/**
 * Produces navigation queries.
 */
public class WhereTermsMaker extends CsqlWhereTermsMaker {
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
            return new FieldCompTerm(f, i, ">");
        };
        C[1] = (f, i, m) -> {
            return new FieldCompTerm(f, i, ">=");
        };
        C[2] = (f, i, m) -> {
            return new FieldCompTerm(f, i, "=");
        };
        C[3] = (f, i, m) -> {
            return new FieldCompTerm(f, i, "<=");
        };
        C[4] = (f, i, m) -> {
            return new FieldCompTerm(f, i, "<");
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
            return OrTerm.construct(new FieldCompTerm(f, i, "<="), new IsNull(f));
        };
        C[24] = (f, i, m) -> {
            return OrTerm.construct(new FieldCompTerm(f, i, "<"), new IsNull(f));
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
            return OrTerm.construct(new FieldCompTerm(f, i, ">"), new IsNull(f));
        };
        C[31] = (f, i, m) -> {
            return OrTerm.construct(new FieldCompTerm(f, i, ">="), new IsNull(f));
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

    public WhereTermsMaker(WhereMakerParamsProvider paramsProvider) {
        this.paramsProvider = paramsProvider;
    }

    static int ind(boolean nullable, boolean nf, boolean isNull, int op) {
        return ((nullable ? 4 : 0) + (nf ? 0 : 2) + (isNull ? 1 : 0)) * 5 + op;
    }

    /**
     * Gets WHERE clause for single record with respect to other filters on a
     * record.
     *
     * @param t  Table meta.
     * @return
     */
    public WhereTerm getHereWhereTerm(BasicTable t) {
        return AndTerm.construct(getPKWhereTerm(t), getWhereTerm());
    }

    /**
     * Gets WHERE clause for filtered row set.
     *
     * @return
     */
    public WhereTerm getWhereTerm() {
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

        WhereTerm l = r == null ? AlwaysTrue.TRUE : r;
        r = paramsProvider.complexFilter() == null ? AlwaysTrue.TRUE : new ComplexFilterTerm();
        r = AndTerm.construct(l, r);


        l = r == null ? AlwaysTrue.TRUE : r;
        r = paramsProvider.inFilter() == null ? AlwaysTrue.TRUE
                                              : new InTerm(paramsProvider.inFilter(), paramsProvider.dba());
        return AndTerm.construct(l, r);
    }

    /**
     * Gets WHERE clause for navigational term with respect of filters and
     * database settings.
     *
     * @param op  navigation operator: '>', '<', or '='.
     * @return
     */
    public WhereTerm getWhereTerm(char op) {
        paramsProvider.initOrderBy();

        boolean invert;
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


        if (paramsProvider.dba().supportsCortegeComparing()) {
            Set<Boolean> set = new HashSet<>();

            for (boolean b: paramsProvider.descOrders()) {
                set.add(b);
            }

            //Проверки возможности использовать кортежи
            boolean allDescOrdersAreEquals = set.size() == 1;


            if (allDescOrdersAreEquals) {

                boolean allOfSortFieldsAreNotNull = true;
                for (String sortField : paramsProvider.sortFields()) {
                    //process with replaced quotes
                    if (paramsProvider.isNullable(sortField.substring(1, sortField.length() - 1))) {
                        allOfSortFieldsAreNotNull = false;
                        break;
                    }
                }


                if (allOfSortFieldsAreNotNull) {
                    FieldsCortegeTerm fieldsCortegeTerm =
                            new FieldsCortegeTerm(Arrays.asList(paramsProvider.sortFields()));
                    ValuesCortegeTerm valuesCortegeTerm = new ValuesCortegeTerm(
                            Arrays.stream(paramsProvider.sortFieldsIndices()).boxed().collect(Collectors.toList())
                    );

                    String operator = invert ^ paramsProvider.descOrders()[0] ? "<" : ">";

                    return AndTerm.construct(getWhereTerm(),
                            new WhereTermCompareTerm(fieldsCortegeTerm, valuesCortegeTerm, operator));
                }
            }
        }


        int l = paramsProvider.sortFields().length;
        char[] ops = new char[l];
        for (int i = 0; i < l; i++) {
            ops[i] = (invert ^ paramsProvider.descOrders()[i]) ? '<' : '>';
        }

        return AndTerm.construct(getWhereTerm(), getWhereTerm(ops, 0));
    }

    private boolean isNull(int k) {
        return rec[paramsProvider.sortFieldsIndices()[k]] == null;
    }

    private WhereTerm getEqualsWhereTerm(int k) {
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

    private WhereTerm getWhereTerm(char[] ops, int k) {
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

    private boolean treatAsNullable(String fieldName) {
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

    /**
     * Unquotes the name.
     * <p>
     * <b>'name'</b> -> <b>name</b>
     *
     * @param name  name to be unquoted
     * @return
     */
    public static String unquot(String name) {
        return name.substring(1, name.length() - 1);
    }

    /**
     * 'SetFilter' filter term.
     */
    final class FilterTerm extends WhereTerm {
        // unquoted column name
        private final String fieldName;
        private final Filter filter;

        FilterTerm(String fieldName, Filter filter) {
            this.fieldName = fieldName;
            this.filter = filter;
        }

        @Override
        public String getWhere() {
            return "(" + filter.makeWhereClause("\"" + fieldName + "\"", paramsProvider.dba()) + ")";
        }

        @Override
        public void programParams(List<ParameterSetter> program, QueryBuildingHelper queryBuildingHelper) {
            // do nothing - no parameters
        }

    }

    /**
     * A term for a complex (CelestaSQL) filter.
     */
    final class ComplexFilterTerm extends WhereTerm {

        @Override
        public String getWhere() {
            return "(" + paramsProvider.complexFilter().getSQL(paramsProvider.dba()) + ")";
        }

        @Override
        public void programParams(List<ParameterSetter> program, QueryBuildingHelper queryBuildingHelper) {
            // do nothing - no parameters
        }

    }

}
