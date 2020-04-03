package ru.curs.celesta.score;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class ParameterizedViewSelectStmt extends ViewSelectStmt {

    final ParameterizedView view;
    private Set<String> unusedParameters;

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
            unusedParameters = paramResolveResult.getUnusedParameters();
            whereCondition.validateTypes();
            view.parameterRefsWithOrder.addAll(paramResolveResult.getParametersWithUsageOrder());
        }
    }

    public Expr getWhereCondition() {
        return whereCondition;
    }

    Set<String> getUnusedParameters() {
        return unusedParameters;
    }
}
