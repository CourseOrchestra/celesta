package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for all view data elements.
 *
 * @author ioann
 * @since 2017-06-08
 */
public abstract class AbstractView extends DataGrainElement {

    static final Map<Class<? extends Expr>, Function<Expr, Column<?>>> EXPR_CLASSES_AND_COLUMN_EXTRACTORS =
            new HashMap<>();

    static {
        EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(Count.class, (Expr frExpr) -> null);

        EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(FieldRef.class, (Expr frExpr) -> {
            FieldRef fr = (FieldRef) frExpr;
            return fr.getColumn();
        });
        EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(Sum.class, (Expr sumExpr) -> {
            Sum sum = (Sum) sumExpr;
            if (sum.term instanceof BinaryTermOp) {
                return null;
            }
            FieldRef fr = (FieldRef) sum.term;
            return fr.getColumn();
        });
    }

    private final List<AbstractSelectStmt> segments = new ArrayList<>();

    public final List<AbstractSelectStmt> getSegments() {
        return segments;
    }

    public AbstractView(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name);
    }

    abstract String viewType();

    /**
     * Writes SELECT script to the stream.
     *
     * @param bw  output stream
     * @param gen SQL generator (visitor)
     * @throws IOException if writing to stream fails
     */
    public void selectScript(final PrintWriter bw, SQLGenerator gen) throws IOException {
        BWWrapper bww = new BWWrapper();

        for (int i = 0; i < segments.size(); i++) {
            AbstractSelectStmt viewSegment = segments.get(i);
            if (i > 0) {
                bw.println(" union all ");
            }
            viewSegment.writeSelectPart(bw, gen, bww);
            viewSegment.writeFromPart(bw, gen);
            viewSegment.writeWherePart(bw, gen);
            viewSegment.writeGroupByPart(bw, gen);
        }
    }

    abstract AbstractSelectStmt newSelectStatement();

    final AbstractSelectStmt addSelectStatement() {
        AbstractSelectStmt result = newSelectStatement();
        segments.add(result);
        return result;
    }

    /**
     * Finalizes view parsing, resolving field references and checking expression types.
     *
     * @throws ParseException Error on types checking or reference resolving.
     */

    void finalizeParsing() throws ParseException {
        //This should never happen: at least one segment must be present
        //before this method is called
        if (segments.size() == 0) {
            throw new IllegalStateException();
        }

        Expr[] first = segments.get(0).columns.values().toArray(new Expr[0]);
        for (int i = 1; i < segments.size(); i++) {
            Expr[] current = segments.get(i).columns.values().toArray(new Expr[0]);
            if (first.length != current.length) {
                throw new ParseException(
                        String.format("Each UNION query in %s '%s.%s' must have the same number of columns",
                                viewType(), getGrain().getName(), getName()));

            }
            for (int j = 0; j < first.length; j++) {
                try {
                    current[j].assertType(first[j].getMeta().getColumnType());
                    first[j].getMeta().setNullable(
                            first[j].getMeta().isNullable()
                                    | current[j].getMeta().isNullable());
                } catch (ParseException e) {
                    throw new ParseException(
                            String.format("UNION types in %s '%s.%s' must match. %s",
                                    viewType(), getGrain().getName(), getName(),
                                    e.getMessage()));
                }
            }
        }
    }

    /**
     * Returns a map of columns of the view.
     *
     * @return
     */
    public abstract Map<String, ? extends ColumnMeta<?>> getColumns();


    /**
     * Returns column index by column name.
     */
    @Override
    public int getColumnIndex(String name) {
        int i = -1;
        for (String c : getColumns().keySet()) {
            i++;
            if (c.equals(name)) {
                return i;
            }
        }
        return i;
    }

    public final Map<String, Expr> getAggregateColumns() {
        if (segments.size() > 0) {
            return segments.get(0).columns.entrySet().stream()
                    .filter(e -> e.getValue() instanceof Aggregate)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey, Map.Entry::getValue,
                            (o, o2) -> {
                                throw new IllegalStateException(String.format("Duplicate key %s", o));
                            }, LinkedHashMap::new));
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Returns column reference by column name.
     *
     * @param colName Column name.
     * @return
     */
    public Column<?> getColumnRef(String colName) {
        if (segments.size() > 0) {
            Expr expr = segments.get(0).columns.get(colName);
            return EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
        } else {
            return null;
        }
    }

    /**
     * Wrapper for automatic line-breaks.
     */
    static class BWWrapper {
        private static final int LINE_SIZE = 80;
        private static final String PADDING = "    ";
        private int l = 0;

        void append(String s, PrintWriter bw) throws IOException {
            bw.write(s);
            l += s.length();
            if (l >= LINE_SIZE) {
                bw.println();
                bw.write(PADDING);
                l = PADDING.length();
            }
        }
    }

}
