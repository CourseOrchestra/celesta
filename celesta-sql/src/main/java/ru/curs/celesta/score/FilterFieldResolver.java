package ru.curs.celesta.score;

/**
 * Auxiliary class for correctness validation of references to field names in
 * complex filters.
 */
final class FilterFieldResolver extends ExprVisitor {
    private final DataGrainElement ge;

    public FilterFieldResolver(DataGrainElement ge) {
        this.ge = ge;
    }

    @Override
    void visitFieldRef(FieldRef expr) throws ParseException {
        if (expr.getTableNameOrAlias() != null) {
            throw new ParseException(
                    String.format(
                            "Invalid column reference %s.%s: no table references allowed in complex filters.",
                            expr.getTableNameOrAlias(), expr.getColumnName()));
        }
        if (!ge.getColumns().containsKey(expr.getColumnName())) {
            throw new ParseException(
                    String.format(
                            "Invalid complex filter: table or view \"%s\" does not contain column \"%s\"",
                            ge.getName(), expr.getColumnName()));
        }
    }

}
