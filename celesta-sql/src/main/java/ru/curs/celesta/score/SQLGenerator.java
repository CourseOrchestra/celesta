package ru.curs.celesta.score;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Generator class of SQL expressions.
 */
public class SQLGenerator extends ExprVisitor {
    private final Deque<String> stack = new LinkedList<>();

    /**
     * Returns SQL view of SQL expression.
     *
     * @param e expression
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
    final void visitBetween(Between expr) {
        String right2 = stack.pop();
        String right1 = stack.pop();
        String left = stack.pop();
        stack.push(String.format("%s BETWEEN %s AND %s", left, right1, right2));
    }

    @Override
    final void visitBinaryLogicalOp(BinaryLogicalOp expr) {
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
    final void visitBinaryTermOp(BinaryTermOp expr) {
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
    final void visitFieldRef(FieldRef expr) {
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
    void visitParameterRef(ParameterRef expr) {
        stack.push(paramLiteral(expr.getName()));
    }

    @Override
    final void visitIn(In expr) {
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
    final void visitIsNull(IsNull expr) {
        stack.push(stack.pop() + " IS NULL");
    }

    @Override
    final void visitNotExpr(NotExpr expr) {
        stack.push("NOT " + stack.pop());
    }

    @Override
    final void visitRealLiteral(RealLiteral expr) {
        stack.push(expr.getLexValue());
    }

    @Override
    final void visitIntegerLiteral(IntegerLiteral expr) {
        stack.push(expr.getLexValue());
    }

    @Override
    final void visitParenthesizedExpr(ParenthesizedExpr expr) {
        stack.push("(" + stack.pop() + ")");
    }

    @Override
    final void visitRelop(Relop expr) {
        String right = stack.pop();
        String left = stack.pop();
        stack.push(left + Relop.OPS[expr.getRelop()] + right);
    }

    @Override
    final void visitTextLiteral(TextLiteral expr) {
        String val = checkForDate(expr.getLexValue());
        stack.push(val);
    }

    @Override
    final void visitBooleanLiteral(BooleanLiteral expr) {
        stack.push(boolLiteral(expr.getValue()));
    }

    @Override
    void visitUpper(Upper expr) {
        stack.push("UPPER(" + stack.pop() + ")");
    }

    @Override
    void visitLower(Lower expr) {
        stack.push("LOWER(" + stack.pop() + ")");
    }

    @Override
    void visitSubstr(Substr expr) {
        String len = stack.pop();
        String from = stack.pop();
        String arg = stack.pop();
        stack.push(substring(arg, from, len));
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

    final void visitUnaryMinus(UnaryMinus expr) {
        stack.push("-" + stack.pop());
    }

    @Override
    final void visitGetDate(GetDate expr) {
        stack.push(getDate());
    }


    protected boolean quoteNames() {
        return true;
    }

    protected String concat() {
        return " || ";
    }

    protected String substring(String arg, String from, String len) {
        return String.format("SUBSTRING(%s from %s for %s)", arg, from, len);
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
    void visitCount(Count expr) {
        stack.push("COUNT(*)");
    }

    @Override
    void visitSum(Sum expr) {
        stack.push("SUM(" + stack.pop() + ")");
    }

    @Override
    void visitMax(Max expr) {
        stack.push("MAX(" + stack.pop() + ")");
    }

    @Override
    void visitMin(Min expr) {
        stack.push("MIN(" + stack.pop() + ")");
    }

}
