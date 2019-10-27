package ru.curs.celesta.score;

import java.util.List;

/**
 * AND/OR.
 */
public final class BinaryLogicalOp extends LogicValuedExpr {
    public static final int AND = 0;
    public static final int OR = 1;
    public static final String[] OPS = { " AND ", " OR " };

    private final int operator;
    private final List<Expr> operands;

    BinaryLogicalOp(int operator, List<Expr> operands) throws ParseException {
        if (operator < 0 || operator >= OPS.length) {
            throw new IllegalArgumentException();
        }
        if (operands.isEmpty()) {
            throw new IllegalArgumentException();
        }
        // all operands should be logical
        for (Expr e : operands) {
            if (e.getMeta().getColumnType() != ViewColumnType.LOGIC) {
                throw new ParseException(
                    String.format("Expression '%s' is expected to be logical condition.", e.getCSQL()));
            }
        }
        this.operands = operands;
        this.operator = operator;
    }

    /**
     * Returns the operator.
     *
     * @return
     */
    public int getOperator() {
        return operator;
    }

    /**
     * Returns the operands.
     *
     * @return
     */
    public List<Expr> getOperands() {
        return operands;
    }

    @Override
    public List<Expr> getAllOperands() {
        return this.getOperands();
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        for (Expr e : operands) {
            e.accept(visitor);
        }
        visitor.visitBinaryLogicalOp(this);
    }
}
