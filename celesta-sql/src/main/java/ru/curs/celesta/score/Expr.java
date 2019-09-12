package ru.curs.celesta.score;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ru.curs.celesta.dbutils.QueryBuildingHelper;

/**
 * Scalar SQL expression.
 */
public abstract class Expr {

    final void assertType(ViewColumnType t) throws ParseException {
        // INT and REAL are both numeric types, so they are comparable
        final ViewColumnType columnType = getMeta().getColumnType();
        if (t == ViewColumnType.INT || t == ViewColumnType.REAL || t == ViewColumnType.DECIMAL) {
            if (!(columnType == ViewColumnType.INT || columnType == ViewColumnType.REAL
                    || columnType == ViewColumnType.DECIMAL)) {
                throw new ParseException(
                        String.format("Expression '%s' is expected to be of numeric type, but it is %s", getCSQL(),
                                getMeta().toString()));
            }
        } else {
            if (columnType != t) {
                throw new ParseException(String.format("Expression '%s' is expected to be of %s type, but it is %s",
                        getCSQL(), t.toString(), getMeta().toString()));
            }
        }
    }

    /**
     * Returns a Celesta-SQL view of the expression.
     *
     * @return
     */
    public final String getCSQL() {
        SQLGenerator gen = new SQLGenerator();
        return gen.generateSQL(this);
    }

    /**
     * Returns an SQL view of the expression in dialect of current DB.
     *
     * @param dba  DB adapter.
     * @return
     */
    public final String getSQL(QueryBuildingHelper dba) {
        SQLGenerator gen = dba.getViewSQLGenerator();
        return gen.generateSQL(this);
    }

    /**
     * Returns the expression type.
     *
     * @return
     */
    public abstract ViewColumnMeta<?> getMeta();

    /**
     * Resolves references to the fields of tables using the context of the defined
     * tables with their aliases.
     *
     * @param tables  list of tables.
     * @throws ParseException  in case if a reference can't be resolved
     */
    final void resolveFieldRefs(List<TableRef> tables) throws ParseException {
        FieldResolver r = new FieldResolver(tables);
        accept(r);
    }

    /**
     * Resolves references to the fields of tables using the context of current
     * object of the score.
     *
     * @param ge  table or view
     * @throws ParseException  in case if a reference can't be resolved
     */
    public final void resolveFieldRefs(GrainElement ge) throws ParseException {
        if (ge instanceof DataGrainElement) {
            FilterFieldResolver fr = new FilterFieldResolver((DataGrainElement) ge);
            accept(fr);
        }
    }

    final ParameterResolverResult resolveParameterRefs(Map<String, Parameter> parameters) throws ParseException {
        ParameterResolver r = new ParameterResolver(parameters);
        accept(r);
        return r.getResult();
    }

    /**
     * Validates the types of all subexpressions in the expression.
     *
     * @throws ParseException  in case if types check fails.
     */
    final void validateTypes() throws ParseException {
        TypeChecker c = new TypeChecker();
        accept(c);
    }

    /**
     * Accepts a visitor during a traversal of the parse tree for solving tasks of
     * types validation, code generation etc.
     *
     * @param visitor  universal visitor element performing tasks with the parse tree.
     * @throws ParseException  semantic error during the tree traversal
     */
    abstract void accept(ExprVisitor visitor) throws ParseException;
}

/**
 * Expression in parentheses.
 */
final class ParenthesizedExpr extends Expr {
    private final Expr parenthesized;

    public ParenthesizedExpr(Expr parenthesized) {
        this.parenthesized = parenthesized;
    }

    /**
     * Returns the expression encompassed by parentheses.
     *
     * @return
     */
    public Expr getParenthesized() {
        return parenthesized;
    }

    @Override
    public ViewColumnMeta<?> getMeta() {
        return parenthesized.getMeta();
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        parenthesized.accept(visitor);
        visitor.visitParenthesizedExpr(this);
    }
}


/**
 * ... IN (..., ..., ...).
 */
final class In extends LogicValuedExpr {
    private final Expr left;
    private final List<Expr> operands;

    In(Expr left, List<Expr> operands) {
        this.operands = operands;
        this.left = left;
    }

    /**
     * Returns the operator.
     *
     * @return
     */
    public Expr getLeft() {
        return left;
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
        return Stream.of(Arrays.asList(left), this.getOperands())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        left.accept(visitor);
        for (Expr operand : operands) {
            operand.accept(visitor);
        }
        visitor.visitIn(this);
    }

}

/**
 * BETWEEN.
 */
final class Between extends LogicValuedExpr {
    private final Expr left;
    private final Expr right1;
    private final Expr right2;

    public Between(Expr left, Expr right1, Expr right2) {
        this.left = left;
        this.right1 = right1;
        this.right2 = right2;
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
     * Returns the part <em>from...</em>.
     *
     * @return
     */
    public Expr getRight1() {
        return right1;
    }

    /**
     * Returns the part <em>to...</em>.
     *
     * @return
     */
    public Expr getRight2() {
        return right2;
    }

    @Override
    public List<Expr> getAllOperands() {
        return Arrays.asList(this.getLeft(), this.getRight1(), this.getRight2());
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        left.accept(visitor);
        right1.accept(visitor);
        right2.accept(visitor);
        visitor.visitBetween(this);
    }

}

/**
 * IS NULL.
 */
final class IsNull extends LogicValuedExpr {
    private final Expr expr;

    IsNull(Expr expr) throws ParseException {
        if (expr.getMeta().getColumnType() == ViewColumnType.LOGIC) {
            throw new ParseException(String.format(
                    "Expression '%s' is logical condition and cannot be an argument of IS NULL operator.", getCSQL()));
        }
        this.expr = expr;
    }

    /**
     * The expression that <em>IS NULL</em> is taken from.
     *
     * @return
     */
    public Expr getExpr() {
        return expr;
    }

    @Override
    public List<Expr> getAllOperands() {
        return Arrays.asList(this.getExpr());
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        expr.accept(visitor);
        visitor.visitIsNull(this);
    }
}

/**
 * NOT.
 */
final class NotExpr extends LogicValuedExpr {
    private final Expr expr;

    NotExpr(Expr expr) throws ParseException {
        this.expr = expr;
    }

    /**
     * The expression that <em>NOT</em> is taken from.
     *
     * @return
     */
    public Expr getExpr() {
        return expr;
    }

    @Override
    public List<Expr> getAllOperands() {
        return Arrays.asList(getExpr());
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        expr.accept(visitor);
        visitor.visitNotExpr(this);
    }
}


/**
 * +, -, *, /.
 */
final class BinaryTermOp extends Expr {
    public static final int PLUS = 0;
    public static final int MINUS = 1;
    public static final int TIMES = 2;
    public static final int OVER = 3;
    public static final int CONCAT = 4;
    public static final String[] OPS = { " + ", " - ", " * ", " / ", " || " };

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

/**
 * Unary minus.
 */
final class UnaryMinus extends Expr {
    private final Expr arg;

    public UnaryMinus(Expr arg) {
        this.arg = arg;
    }

    public Expr getExpr() {
        return arg;
    }

    @Override
    public ViewColumnMeta<?> getMeta() {
        // Real or Integer
        return arg.getMeta();
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        arg.accept(visitor);
        visitor.visitUnaryMinus(this);
    }
}

/**
 * Base class for literal expressions.
 */
abstract class Literal extends Expr {
    private final ViewColumnType type;
    private ViewColumnMeta<?> meta;

    Literal(ViewColumnType type) {
        this.type = type;
    }

    @Override
    public final ViewColumnMeta<?> getMeta() {
        if (meta == null) {
            meta = new ViewColumnMeta<>(type);
            meta.setNullable(false);
        }
        return meta;
    }
}

/**
 * Numeric numeral with a floating point.
 */
final class RealLiteral extends Literal {
    private final String lexValue;

    RealLiteral(String lexValue) {
        super(ViewColumnType.REAL);
        this.lexValue = lexValue;
    }

    /**
     * Returns the lexical value.
     *
     * @return
     */
    public String getLexValue() {
        return lexValue;
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitRealLiteral(this);
    }
}

/**
 * Numeric numeral.
 */
final class IntegerLiteral extends Literal {
    private final String lexValue;

    IntegerLiteral(String lexValue) {
        super(ViewColumnType.INT);
        this.lexValue = lexValue;
    }

    /**
     * Returns the lexical value.
     *
     * @return
     */
    public String getLexValue() {
        return lexValue;
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitIntegerLiteral(this);
    }
}

/**
 * {@code TRUE} or {@code FALSE} literal.
 */
final class BooleanLiteral extends Literal {

    private final boolean val;

    BooleanLiteral(boolean val) {
        super(ViewColumnType.BIT);
        this.val = val;
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitBooleanLiteral(this);
    }

    public boolean getValue() {
        return val;
    }

}

/**
 * Text literal.
 */
final class TextLiteral extends Literal {
    private final String lexValue;

    TextLiteral(String lexValue) {
        super(ViewColumnType.TEXT);
        this.lexValue = lexValue;
    }

    /**
     * Returns the lexical value.
     *
     * @return
     */
    public String getLexValue() {
        return lexValue;
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitTextLiteral(this);
    }

}

final class GetDate extends Expr {
    @Override
    public ViewColumnMeta<?> getMeta() {
        return new ViewColumnMeta<>(ViewColumnType.DATE);
    }

    @Override
    void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitGetDate(this);
    }
}


abstract class Aggregate extends Expr {

}

