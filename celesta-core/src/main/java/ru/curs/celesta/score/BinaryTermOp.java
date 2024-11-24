package ru.curs.celesta.score;

import java.util.List;

/**
 * +, -, *, /.
 */
public final class BinaryTermOp extends Expr {
    /**
     * Index for addition operator.
     */
    public static final int PLUS = 0;
    /**
     * Index for subtraction operator.
     */
    public static final int MINUS = 1;
    /**
     * Index for multiplication operator.
     */
    public static final int TIMES = 2;
    /**
     * Index for division operator.
     */
    public static final int OVER = 3;
    /**
     * Index for string concatenation operator.
     */
    public static final int CONCAT = 4;

    /**
     * Array of operators.
     */
    static final String[] OPS = {" + ", " - ", " * ", " / ", " || "};

    private ViewColumnMeta<?> meta;

    private final int operator;
    private final List<Expr> operands;

    BinaryTermOp(int operator, List<Expr> operands) {
        if (operator < 0 || operator >= OPS.length) {
            throw new IllegalArgumentException();
        }
        if (operands.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.operands = operands;
        this.operator = operator;
    }

    /**
     * Returns the operator.
     *
     */
    public int getOperator() {
        return operator;
    }

    /**
     * Returns the operands.
     *
     */
    public List<Expr> getOperands() {
        return operands;
    }

    @Override
    public ViewColumnMeta<?> getMeta() {
        if (meta == null) {
            cases: switch (operator) {
                case CONCAT: // ||
                    meta = new ViewColumnMeta<>(ViewColumnType.TEXT);
                    break;
                case OVER: // /
                    meta = new ViewColumnMeta<>(ViewColumnType.REAL);
                    break;
                default: // +, -, *
                    for (Expr o : operands) {
                        if (o.getMeta().getColumnType() == ViewColumnType.REAL) {
                            meta = new ViewColumnMeta<>(ViewColumnType.REAL);
                            break cases;
                        }
                        if (o.getMeta().getColumnType() == ViewColumnType.DECIMAL) {
                            meta = new ViewColumnMeta<>(ViewColumnType.DECIMAL);
                            break cases;
                        }
                    }
                    meta = new ViewColumnMeta<>(ViewColumnType.INT);
                    break;
            }

            // now checking for nullability
            boolean n = false;
            for (Expr o : operands) {
                if (o.getMeta().isNullable()) {
                    n = true;
                    break;
                }
            }
            meta.setNullable(n);
        }
        return meta;
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        for (Expr e : operands) {
            e.accept(visitor);
        }
        visitor.visitBinaryTermOp(this);
    }
}
