package ru.curs.celesta.score;

import java.util.ArrayList;
import java.util.List;

public final class ParameterizedViewSelectStmt extends ViewSelectStmt {

    final ParameterizedView view;

    public ParameterizedViewSelectStmt(ParameterizedView view) {
        super(view);
        this.view = view;
    }

    @Override
    void finalizeWhereConditionParsing() throws ParseException {
        List<TableRef> t = new ArrayList<>(tables.values());
        if (whereCondition != null) {
            whereCondition.resolveFieldRefs(t);
            ParameterResolverResult paramResolveResult = whereCondition.resolveParameterRefs(view.parameters);

            if (!paramResolveResult.getUnusedParameters().isEmpty()) {
                String unusedParametersStr = String.join(", ", paramResolveResult.getUnusedParameters());
                throw new ParseException(String.format("%s '%s' contains not used parameters %s.",
                        view.viewType(), view.getName(), unusedParametersStr));
            }
            whereCondition.validateTypes();

            view.parameterRefsWithOrder.addAll(paramResolveResult.getParametersWithUsageOrder());
        }
    }

    public Expr getWhereCondition() {
        return whereCondition;
    }
}
