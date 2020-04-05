package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A segment of a UNION chain in a SQL UNION ALL query.
 */
public abstract class AbstractSelectStmt {
    final AbstractView view;

    final Map<String, Expr> columns = new LinkedHashMap<>();
    final Map<String, FieldRef> groupByColumns = new LinkedHashMap<>();

    boolean distinct;

    final Map<String, TableRef> tables = new LinkedHashMap<>();

    public AbstractSelectStmt(AbstractView view) {
        this.view = view;
    }


    /**
     * Writes SELECT part to the stream.
     *
     * @param bw  output stream
     * @param gen SQL generator (visitor)
     * @param bww line break wrapper
     * @throws IOException if writing to stream fails
     */
    void writeSelectPart(final PrintWriter bw, SQLGenerator gen, AbstractView.BWWrapper bww) throws IOException {
        bww.append("  select ", bw);
        if (distinct) {
            bww.append("distinct ", bw);
        }

        boolean cont = false;
        for (Map.Entry<String, Expr> e : columns.entrySet()) {
            if (cont) {
                bww.append(", ", bw);
            }
            String st = gen.generateSQL(e.getValue()) + " as ";
            if (gen.quoteNames()) {
                st = st + "\"" + e.getKey() + "\"";
            } else {
                st = st + e.getKey();
            }
            bww.append(st, bw);
            cont = true;
        }
        bw.println();
    }

    /**
     * Writes FROM part to the stream.
     *
     * @param bw  output stream
     * @param gen SQL generator (visitor)
     * @throws IOException if writing to stream fails
     */
    void writeFromPart(final PrintWriter bw, SQLGenerator gen) throws IOException {
        bw.write("  from ");
        boolean cont = false;
        for (TableRef tRef : tables.values()) {
            if (cont) {
                bw.println();
                bw.printf("    %s ", tRef.getJoinType().toString());
                bw.write("join ");
            }
            bw.write(gen.tableName(tRef));
            if (cont) {
                bw.write(" on ");
                bw.write(gen.generateSQL(tRef.getOnExpr()));
            }
            cont = true;
        }
    }

    /**
     * Writes WHERE part to the stream.
     *
     * @param bw  output stream
     * @param gen SQL generator (visitor)
     * @throws IOException if writing to stream fails
     */
    void writeWherePart(final PrintWriter bw, SQLGenerator gen) throws IOException {
    }

    /**
     * Writes GROUP BY part to the stream.
     *
     * @param bw  output stream
     * @param gen SQL generator (visitor)
     * @throws IOException if writing to stream fails
     */
    void writeGroupByPart(final PrintWriter bw, SQLGenerator gen) throws IOException {
        if (!groupByColumns.isEmpty()) {
            bw.println();
            bw.write(" group by ");

            int countOfProcessed = 0;
            for (Expr field : groupByColumns.values()) {
                bw.write(gen.generateSQL(field));

                if (++countOfProcessed != groupByColumns.size()) {
                    bw.write(", ");
                }
            }

        }
    }

    /**
     * Adds a column to the view.
     *
     * @param alias column alias.
     * @param expr  column expression.
     * @throws ParseException Non-unique alias name or some other semantic error
     */
    void addColumn(String alias, Expr expr) throws ParseException {
        if (expr == null) {
            throw new IllegalArgumentException();
        }

        if (alias == null || alias.isEmpty()) {
            throw new ParseException(String.format("%s '%s' contains a column with undefined alias.",
                    view.viewType(), view.getName()));
        }
        alias = view.getGrain().getScore().getIdentifierParser().parse(alias);
        if (columns.containsKey(alias)) {
            throw new ParseException(String.format(
                    "%s '%s' already contains column with name or alias '%s'. Use unique aliases for %s columns.",
                    view.viewType(), view.getName(), alias, view.viewType()));
        }

        columns.put(alias, expr);
    }

    /**
     * Adds a column to the "GROUP BY" clause of the view.
     *
     * @param fr Column expression.
     * @throws ParseException Non-unique alias name, missing column in selection or some other semantic error
     */
    void addGroupByColumn(FieldRef fr) throws ParseException {
        if (fr == null) {
            throw new IllegalArgumentException();
        }

        String alias = fr.getColumnName();

        if (groupByColumns.containsKey(alias)) {
            throw new ParseException(String.format(
                    "Duplicate column '%s' in GROUP BY expression for %s '%s.%s'.",
                    alias, view.viewType(), view.getGrain().getName(), view.getName()));
        }

        Expr existedColumn = columns.get(fr.getColumnName());

        if (existedColumn == null) {
            throw new ParseException("Couldn't resolve column ref " + fr.getColumnName());
        }

        if (existedColumn.getClass().equals(FieldRef.class)) {
            FieldRef existedColumnFr = (FieldRef) existedColumn;
            fr.setTableNameOrAlias(existedColumnFr.getTableNameOrAlias());
            fr.setColumnName(existedColumnFr.getColumnName());
            fr.setColumn(existedColumnFr.getColumn());
        }

        groupByColumns.put(alias, fr);
    }


    /**
     * Adds a table reference to the view.
     *
     * @param ref Table reference.
     * @throws ParseException Non-unique alias or some other semantic error.
     */
    void addFromTableRef(TableRef ref) throws ParseException {
        if (ref == null) {
            throw new IllegalArgumentException();
        }

        String alias = ref.getAlias();
        if (alias == null || alias.isEmpty()) {
            throw new ParseException(String.format("%s '%s' contains a table with undefined alias.",
                    view.viewType(), view.getName()));
        }
        if (tables.containsKey(alias)) {
            throw new ParseException(String.format(
                    "%s, '%s' already contains table with name or alias '%s'. Use unique aliases for %s tables.",
                    view.viewType(), view.getName(), alias, view.viewType()));
        }

        tables.put(alias, ref);

        Expr onCondition = ref.getOnExpr();
        if (onCondition != null) {
            onCondition.resolveFieldRefs(new ArrayList<>(tables.values()));
            onCondition.validateTypes();
        }
    }

    void finalizeColumnsParsing() throws ParseException {
        List<TableRef> t = new ArrayList<>(tables.values());
        for (Expr e : columns.values()) {
            e.resolveFieldRefs(t);
            e.validateTypes();
        }
    }

    /**
     * Sets where condition for SQL query.
     *
     * @param whereCondition where condition.
     * @throws ParseException if expression type is incorrect.
     */
    abstract void setWhereCondition(Expr whereCondition) throws ParseException;

    /**
     * Finalizes view parsing, resolving field references and checking expression types.
     *
     * @throws ParseException Error on types checking or reference resolving.
     */
    abstract void finalizeParsing() throws ParseException;

    void finalizeGroupByParsing() throws ParseException {
        //Check that columns which were not used for aggregation are mentioned in GROUP BY clause
        Set<String> aggregateAliases = columns.entrySet().stream()
                .filter(e -> e.getValue() instanceof Aggregate)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (!((aggregateAliases.isEmpty() || aggregateAliases.size() == columns.size())
                && groupByColumns.isEmpty())) {

            //Iterate by columns which are not aggregates and throw an exception
            // if at least one of them is absent from groupByColumns
            boolean hasErrorOpt = columns.entrySet().stream()
                    .anyMatch(e -> !(e.getValue() instanceof Aggregate) && !groupByColumns.containsKey(e.getKey()));
            if (hasErrorOpt) {
                throw new ParseException(String.format("%s '%s.%s' contains a column(s) "
                                + "which was not specified in aggregate function and GROUP BY expression.",
                        view.viewType(), view.getGrain().getName(), view.getName()));
            }
        }
    }


    /**
     * Whether DISTINCT keyword was used in the view query.
     *
     * @return
     */
    boolean isDistinct() {
        return distinct;
    }

    /**
     * Sets the use of keyword DISTINCT in the view query.
     *
     * @param distinct Whether the query has the form of SELECT DISTINCT.
     */
    void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    final Grain getGrain() {
        return view.getGrain();
    }

}
