package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

class ViewSelectStmt extends AbstractSelectStmt {
    protected Expr whereCondition;

    public ViewSelectStmt(AbstractView view) {
        super(view);
    }

    @Override
    void setWhereCondition(Expr whereCondition) throws ParseException {
        if (whereCondition != null) {
            List<TableRef> t = new ArrayList<>(tables.values());
            whereCondition.resolveFieldRefs(t);
            whereCondition.assertType(ViewColumnType.LOGIC);
        }
        this.whereCondition = whereCondition;
    }

    @Override
    void finalizeParsing() throws ParseException {
        finalizeColumnsParsing();
        finalizeWhereConditionParsing();
        finalizeGroupByParsing();
    }

    void finalizeWhereConditionParsing() throws ParseException {
        List<TableRef> t = new ArrayList<>(tables.values());
        if (whereCondition != null) {
            whereCondition.resolveFieldRefs(t);
            whereCondition.validateTypes();
        }
    }

    @Override
    final void writeWherePart(PrintWriter bw, SQLGenerator gen) throws IOException {
        if (whereCondition != null) {
            bw.println();
            bw.write("  where ");
            bw.write(gen.generateSQL(whereCondition));
        }
    }
}
