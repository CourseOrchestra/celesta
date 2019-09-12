package ru.curs.celesta.score;

import java.util.Arrays;
import java.util.List;

/**
 * Relation (>=, <=, <>, =, <, >).
 */
public final class Relop extends LogicValuedExpr {
    /**
     * >.
     */
    public static final int GT = 0;
    /**
     * <.
     */
    public static final int LS = 1;
    /**
     * >=.
     */
    public static final int GTEQ = 2;
    /**
     * <=.
     */
    public static final int LSEQ = 3;
    /**
     * <>.
     */
    public static final int NTEQ = 4;
    /**
     * =.
     */
    public static final int EQ = 5;

    /**
     * LIKE.
     */
    public static final int LIKE = 6;

    static final String[] OPS = { " > ", " < ", " >= ", " <= ", " <> ", " = ", " LIKE " };

    private final Expr left;
    private final Expr right;
    private final int relop;

    public Relop(Expr left, Expr right, int relop) {
        if (relop < 0 || relop >= OPS.length) {
            throw new IllegalArgumentException();
        }
        this.left = left;
        this.right = right;
        this.relop = relop;
    }

    /**
     * Returns the left part.
     *
     * @return
     */
    public Expr getLeft() {
        return left;
    }

    /**
     * Returns the right part.
     *
     * @return
     */
    public Expr getRight() {
        return right;
    }

    /**
     * Returns the relation.
     *
     * @return
     */
    public int getRelop() {
        return relop;
    }

    @Override
    public List<Expr> getAllOperands() {
        return Arrays.asList(this.getLeft(), this.getRight());
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        left.accept(visitor);
        right.accept(visitor);
        visitor.visitRelop(this);
    }
}
