package ru.curs.celesta.score;

import java.util.List;

/**
 * Expression with logical value. Celestadoc and nullable is not actual.
 */
public abstract class LogicValuedExpr extends Expr {
    private static final ViewColumnMeta<?> META;

    static {
        META = new ViewColumnMeta<>(ViewColumnType.LOGIC);
        META.setNullable(false);
    }

    public abstract List<Expr> getAllOperands();

    @Override
    public final ViewColumnMeta<?> getMeta() {
        return META;
    }

}
