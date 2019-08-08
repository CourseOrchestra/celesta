package ru.curs.celesta.score;

/**
 * Table reference in SQL query.
 */
public class TableRef {
    /**
     * JOIN type. FULL JOIN isn't supported because of historical reasons,
     * maybe someday it will be added. CROSS JOIN isn't supported for
     * security and speed reasons.
     */
    public enum JoinType {

        /**
         * INNER JOIN.
         */
        INNER, /**
         * LEFT JOIN.
         */
        LEFT, /**
         * RIGHT JOIN.
         */
        RIGHT
    }

    private final BasicTable table;
    private final String alias;

    private JoinType joinType;
    private Expr onExpr;

    public TableRef(BasicTable table, String alias) {
        this.table = table;
        this.alias = alias;
    }

    /**
     * Returns JOIN type.
     *
     * @return
     */
    public JoinType getJoinType() {
        return joinType;
    }

    /**
     * Returns table.
     *
     * @return
     */
    public BasicTable getTable() {
        return table;
    }

    /**
     * Returns condition ON...
     *
     * @return
     */
    public Expr getOnExpr() {
        return onExpr;
    }

    void setOnExpr(Expr onExpr) throws ParseException {
        if (onExpr == null) {
            throw new IllegalArgumentException();
        }
        onExpr.assertType(ViewColumnType.LOGIC);
        this.onExpr = onExpr;

    }

    /**
     * Returns table alias.
     *
     * @return
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Sets JOIN type.
     *
     * @param joinType  JOIN type
     */
    void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

}
