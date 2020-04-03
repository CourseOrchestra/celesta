package ru.curs.celesta.score;

import java.util.Map;

final class MaterializedSelectStmt extends AbstractSelectStmt {
    final MaterializedView view;

    public MaterializedSelectStmt(MaterializedView view) {
        super(view);
        this.view = view;
    }

    @Override
    void setWhereCondition(Expr whereCondition) throws ParseException {
        throw new ParseException(String.format(
                "Exception while parsing materialized view %s.%s Materialized views doesn't support where condition.",
                view.getGrain().getName(), view.getName()));
    }

    @Override
    void finalizeParsing() throws ParseException {

        //Присутствие хотя бы одного агрегатного столбца - обязательное условие
        boolean aggregate = columns.entrySet().stream()
                .anyMatch(e -> e.getValue() instanceof Aggregate);

        if (!aggregate) {
            throw new ParseException(String.format("%s %s.%s must have at least one aggregate column",
                    view.viewType(), view.getGrain().getName(), view.getName()));
        }

        finalizeColumnsParsing();
        finalizeGroupByParsing();
    }

    @Override
    void finalizeColumnsParsing() throws ParseException {
        super.finalizeColumnsParsing();

        for (Map.Entry<String, Expr> entry : columns.entrySet()) {
            String alias = entry.getKey();
            Expr expr = entry.getValue();

            final Column<?> colRef;
            final MaterializedView.MatColFabricFunction matColFabricFunction;

            if (expr instanceof Count) {
                colRef = null;
                matColFabricFunction = MaterializedView.COL_CLASSES_AND_FABRIC_FUNCS.get(IntegerColumn.class);
            } else {
                colRef = MaterializedView.EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
                matColFabricFunction = MaterializedView.COL_CLASSES_AND_FABRIC_FUNCS.get(colRef.getClass());
            }

            if (matColFabricFunction == null) {
                throw new ParseException(String.format(
                        "Unsupported type '%s' of column '%s' in materialized view %s was found",
                        expr.getMeta().getCelestaType(), alias, view.getName()));
            } else {
                Column<?> col = matColFabricFunction.apply(view, colRef, alias);
                if (!(expr instanceof Aggregate)) {
                    view.pk.addElement(col);
                    col.setNullableAndDefault(false, null);
                }
            }
        }
    }

    @Override
    void finalizeGroupByParsing() throws ParseException {
        super.finalizeGroupByParsing();

        for (String alias : groupByColumns.keySet()) {
            Column<?> colRef = ((FieldRef) columns.get(alias)).getColumn();
            if (colRef.isNullable()) {
                throw new ParseException(String.format(
                        "Nullable column %s was found in GROUP BY expression for %s '%s.%s'.",
                        alias, view.viewType(), view.getGrain().getName(), view.getName())
                );
            }
        }
    }

    @Override
    void addFromTableRef(TableRef ref) throws ParseException {

        if (!view.getGrain().equals(ref.getTable().getGrain())) {
            throw new ParseException(String.format("%s '%s.%s' contains a table from another grain.",
                    view.viewType(), view.getGrain().getName(), view.getName()));
        }

        super.addFromTableRef(ref);
    }
}
