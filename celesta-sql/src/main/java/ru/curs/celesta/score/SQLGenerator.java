package ru.curs.celesta.score;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Generator class of SQL expressions.
 */
public class SQLGenerator extends ExprVisitor {
    private Deque<String> stack = new LinkedList<>();

    /**
     * Returns SQL view of SQL expression.
     *
     * @param e expression
     * @return
     */
    final String generateSQL(Expr e) {
        try {
            e.accept(this);
        } catch (ParseException e1) {
            // This should never ever happen
            throw new RuntimeException(e1);
        }
        return stack.pop();
    }

    @Override
    final void visitBetween(Between expr) throws ParseException {
        String right2 = stack.pop();
        String right1 = stack.pop();
        String left = stack.pop();
        stack.push(String.format("%s BETWEEN %s AND %s", left, right1, right2));
    }

    @Override
    final void visitBinaryLogicalOp(BinaryLogicalOp expr) throws ParseException {
        StringBuilder result = new StringBuilder();
        String op = BinaryLogicalOp.OPS[expr.getOperator()];
        LinkedList<String> operands = new LinkedList<>();
        for (int i = 0; i < expr.getOperands().size(); i++) {
            operands.push(stack.pop());
        }
        boolean needOp = false;
        for (String operand : operands) {
            if (needOp) {
                result.append(op);
            }
            result.append(operand);
            needOp = true;
        }
        stack.push(result.toString());
    }

    @Override
    final void visitBinaryTermOp(BinaryTermOp expr) throws ParseException {
        StringBuilder result = new StringBuilder();
        LinkedList<String> operands = new LinkedList<>();
        for (int i = 0; i < expr.getOperands().size(); i++) {
            operands.push(stack.pop());
        }

        if (expr.getOperator() == BinaryTermOp.CONCAT) {
            concat(result, operands);
        } else {
            String op = BinaryTermOp.OPS[expr.getOperator()];
            boolean needOp = false;
            for (String operand : operands) {
                if (needOp) {
                    result.append(op);
                }
                result.append(operand);
                needOp = true;
            }
        }
        stack.push(result.toString());
    }

    @Override
    final void visitFieldRef(FieldRef expr) throws ParseException {
        StringBuilder result = new StringBuilder();

        if (expr.getTableNameOrAlias() != null) {
            if (quoteNames()) {
                result.append("\"");
            }
            result.append(expr.getTableNameOrAlias());
            if (quoteNames()) {
                result.append("\"");
            }
            result.append(".");
        }
        if (quoteNames()) {
            result.append("\"");
        }
        result.append(expr.getColumnName());
        if (quoteNames()) {
            result.append("\"");
        }
        stack.push(result.toString());
    }

    @Override
    void visitParameterRef(ParameterRef expr) throws ParseException {
        stack.push(paramLiteral(expr.getName()));
    }

    @Override
    final void visitIn(In expr) throws ParseException {
        StringBuilder result = new StringBuilder();
        LinkedList<String> operands = new LinkedList<>();
        for (int i = 0; i < expr.getOperands().size(); i++) {
            operands.push(stack.pop());
        }
        result.append(stack.pop());
        result.append(" IN (");
        boolean needComma = false;
        for (String operand : operands) {
            if (needComma) {
                result.append(", ");
            }
            result.append(operand);
            needComma = true;
        }
        result.append(")");
        stack.push(result.toString());
    }

    @Override
    final void visitIsNull(IsNull expr) throws ParseException {
        stack.push(stack.pop() + " IS NULL");
    }

    @Override
    final void visitNotExpr(NotExpr expr) throws ParseException {
        stack.push("NOT " + stack.pop());
    }

    @Override
    final void visitRealLiteral(RealLiteral expr) throws ParseException {
        stack.push(expr.getLexValue());
    }

    @Override
    final void visitIntegerLiteral(IntegerLiteral expr) throws ParseException {
        stack.push(expr.getLexValue());
    }

    @Override
    final void visitParenthesizedExpr(ParenthesizedExpr expr) throws ParseException {
        stack.push("(" + stack.pop() + ")");
    }

    @Override
    final void visitRelop(Relop expr) throws ParseException {
        String right = stack.pop();
        String left = stack.pop();
        stack.push(left + Relop.OPS[expr.getRelop()] + right);
    }

    @Override
    final void visitTextLiteral(TextLiteral expr) throws ParseException {
        String val = checkForDate(expr.getLexValue());
        stack.push(val);
    }

    @Override
    final void visitBooleanLiteral(BooleanLiteral expr) throws ParseException {
        stack.push(boolLiteral(expr.getValue()));
    }

    @Override
    void visitUpper(Upper expr) throws ParseException {
        stack.push("UPPER(" + stack.pop() + ")");
    }

    @Override
    void visitLower(Lower expr) throws ParseException {
        stack.push("LOWER(" + stack.pop() + ")");
    }

    protected String boolLiteral(boolean val) {
        return val ? "true" : "false";
    }

    protected String paramLiteral(String paramName) {
        return "$" + paramName;
    }

    protected String getDate() {
        return "GETDATE()";
    }

    protected String checkForDate(String lexValue) {
        return lexValue;
    }

    final void visitUnaryMinus(UnaryMinus expr) throws ParseException {
        stack.push("-" + stack.pop());
    }

    @Override
    final void visitGetDate(GetDate expr) throws ParseException {
        stack.push(getDate());
    }


    protected boolean quoteNames() {
        return true;
    }

    protected String concat() {
        return " || ";
    }

    protected void concat(StringBuilder result, List<String> operands) {
        boolean needOp = false;
        for (String operand : operands) {
            if (needOp) {
                result.append(concat());
            }
            result.append(operand);
            needOp = true;
        }
    }

    protected String preamble(AbstractView view) {
        return String.format("create or replace view %s as", viewName(view));
    }

    protected String viewName(AbstractView v) {
        return String.format("%s.%s", v.getGrain().getQuotedName(), v.getQuotedName());
    }

    protected String tableName(TableRef t) {
        return String.format("%s.%s as \"%s\"", t.getTable().getGrain().getQuotedName(), t.getTable().getQuotedName(),
                t.getAlias());
    }

    @Override
    void visitCount(Count expr) throws ParseException {
        stack.push("COUNT(*)");
    }

    @Override
    void visitSum(Sum expr) throws ParseException {
        stack.push("SUM(" + stack.pop() + ")");
    }

    @Override
    void visitMax(Max expr) throws ParseException {
        stack.push("MAX(" + stack.pop() + ")");
    }

    @Override
    void visitMin(Min expr) throws ParseException {
        stack.push("MIN(" + stack.pop() + ")");
    }

}
