package ru.curs.celesta.score;

import java.util.List;

/** Скалярное выражение SQL. */
abstract class Expr {
	private final View v;

	Expr(View v) {
		this.v = v;
	}

	final View getView() {
		return v;
	}

	final void assertType(ExprType t) throws ParseException {
		if (getType() != t)
			throw new ParseException(
					String.format(
							"Expression '%s' is expected to be of %s type, but it is %s",
							getCSQL(), t.toString(), getType().toString()));
	}

	/**
	 * Возвращает Celesta-SQL представление выражения.
	 */
	public final String getCSQL() {
		SQLGenerator gen = new SQLGenerator();
		return gen.generateSQL(this);
	}

	/**
	 * Возвращает тип выражения.
	 */
	public abstract ExprType getType();

	/**
	 * Разрешает ссылки на поля таблиц, используя контекст объявленных таблиц с
	 * их псевдонимами.
	 * 
	 * @param tables
	 *            перечень таблиц.
	 * @throws ParseException
	 *             в случае, если ссылка не может быть разрешена.
	 */
	public final void resolveFieldRefs(List<TableRef> tables)
			throws ParseException {
		FieldResolver r = new FieldResolver(tables);
		accept(r);
	}

	/**
	 * Проверяет типы всех входящих в выражение субвыражений.
	 * 
	 * @throws ParseException
	 *             в случае, если контроль типов не проходит.
	 */
	public final void validateTypes() throws ParseException {
		TypeChecker c = new TypeChecker();
		accept(c);
	}

	/**
	 * Принимает посетителя при обходе дерева разбора для решения задач контроля
	 * типов, кодогенерации и проч.
	 * 
	 * @param visitor
	 *            Универсальный элемент visitor, выполняющий задачи с деревом
	 *            разбора.
	 * @throws ParseException
	 *             семантическая ошибка при обходе дерева.
	 */
	public abstract void accept(ExprVisitor visitor) throws ParseException;
}

/**
 * Тип выражения.
 */
enum ExprType {
	LOGIC, NUMERIC, TEXT, DATE, BIT, BLOB, UNDEFINED
}

/**
 * Выражение в скобках.
 */
final class ParenthesizedExpr extends Expr {
	private final Expr parenthesized;

	public ParenthesizedExpr(View v, Expr parenthesized) {
		super(v);
		this.parenthesized = parenthesized;
	}

	/**
	 * Возвращает выражение, заключенное в скобки.
	 */
	public Expr getParenthesized() {
		return parenthesized;
	}

	@Override
	public ExprType getType() {
		return parenthesized.getType();
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		parenthesized.accept(visitor);
		visitor.visitParenthesizedExpr(this);
	}
}

/**
 * Отношение (>=, <=, <>, =, <, >).
 */
final class Relop extends Expr {
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

	static final String[] OPS = { " > ", " < ", " >= ", " <= ", " <> ", " = ",
			" LIKE " };

	private final Expr left;
	private final Expr right;
	private final int relop;

	public Relop(View v, Expr left, Expr right, int relop) {
		super(v);
		if (relop < 0 || relop >= OPS.length)
			throw new IllegalArgumentException();
		this.left = left;
		this.right = right;
		this.relop = relop;
	}

	/**
	 * Левая сторона.
	 */
	public Expr getLeft() {
		return left;
	}

	/**
	 * Правая сторона.
	 */
	public Expr getRight() {
		return right;
	}

	/**
	 * Отношение.
	 */
	public int getRelop() {
		return relop;
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		left.accept(visitor);
		right.accept(visitor);
		visitor.visitRelop(this);
	}
}

/**
 * ... IN (..., ..., ...).
 */
final class In extends Expr {
	private final Expr left;
	private final List<Expr> operands;

	In(View v, Expr left, List<Expr> operands) {
		super(v);
		this.operands = operands;
		this.left = left;
	}

	/**
	 * Оператор.
	 */
	public Expr getLeft() {
		return left;
	}

	/**
	 * Операнды.
	 */
	public List<Expr> getOperands() {
		return operands;
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		left.accept(visitor);
		for (Expr operand : operands)
			operand.accept(visitor);
		visitor.visitIn(this);
	}

}

/**
 * BETWEEN.
 */
final class Between extends Expr {
	private final Expr left;
	private final Expr right1;
	private final Expr right2;

	public Between(View v, Expr left, Expr right1, Expr right2) {
		super(v);
		this.left = left;
		this.right1 = right1;
		this.right2 = right2;
	}

	/**
	 * Левая часть.
	 */
	public Expr getLeft() {
		return left;
	}

	/**
	 * Часть от...
	 */
	public Expr getRight1() {
		return right1;
	}

	/**
	 * Часть до...
	 */
	public Expr getRight2() {
		return right2;
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
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
final class IsNull extends Expr {
	private final Expr expr;

	IsNull(View v, Expr expr) throws ParseException {
		super(v);
		if (expr.getType() == ExprType.LOGIC)
			throw new ParseException(
					String.format(
							"Expression '%s' is logical condition and cannot be an argument of IS NULL operator.",
							getCSQL()));
		this.expr = expr;
	}

	/**
	 * Выражение, от которого берётся IS NULL.
	 */
	public Expr getExpr() {
		return expr;
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
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
final class NotExpr extends Expr {
	private final Expr expr;

	NotExpr(View v, Expr expr) throws ParseException {
		super(v);
		if (expr.getType() != ExprType.LOGIC)
			throw new ParseException(String.format(
					"Expression '%s' is expected to be logical condition.",
					getCSQL()));
		this.expr = expr;
	}

	/**
	 * Выражение, от которого берётся NOT.
	 */
	public Expr getExpr() {
		return expr;
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		expr.accept(visitor);
		visitor.visitNotExpr(this);
	}
}

/**
 * AND/OR.
 */
final class BinaryLogicalOp extends Expr {
	public static final int AND = 0;
	public static final int OR = 1;
	public static final String[] OPS = { " AND ", " OR " };

	private final int operator;
	private final List<Expr> operands;

	BinaryLogicalOp(View v, int operator, List<Expr> operands)
			throws ParseException {
		super(v);
		if (operator < 0 || operator >= OPS.length)
			throw new IllegalArgumentException();
		if (operands.isEmpty())
			throw new IllegalArgumentException();
		// все операнды должны быть логическими
		for (Expr e : operands)
			if (e.getType() != ExprType.LOGIC)
				throw new ParseException(String.format(
						"Expression '%s' is expected to be logical condition.",
						e.getCSQL()));
		this.operands = operands;
		this.operator = operator;
	}

	/**
	 * Оператор.
	 */
	public int getOperator() {
		return operator;
	}

	/**
	 * Операнды.
	 */
	public List<Expr> getOperands() {
		return operands;
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		for (Expr e : operands)
			e.accept(visitor);
		visitor.visitBinaryLogicalOp(this);
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

	private final int operator;
	private final List<Expr> operands;

	BinaryTermOp(View v, int operator, List<Expr> operands) {
		super(v);
		if (operator < 0 || operator >= OPS.length)
			throw new IllegalArgumentException();
		if (operands.isEmpty())
			throw new IllegalArgumentException();

		this.operands = operands;
		this.operator = operator;
	}

	/**
	 * Оператор.
	 */
	public int getOperator() {
		return operator;
	}

	/**
	 * Операнды.
	 */
	public List<Expr> getOperands() {
		return operands;
	}

	@Override
	public ExprType getType() {
		return operator == CONCAT ? ExprType.TEXT : ExprType.NUMERIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		for (Expr e : operands)
			e.accept(visitor);
		visitor.visitBinaryTermOp(this);
	}
}

/**
 * Унарный минус.
 */
final class UnaryMinus extends Expr {
	private final Expr arg;

	public UnaryMinus(View v, Expr arg) {
		super(v);
		this.arg = arg;
	}

	public Expr getExpr() {
		return arg;
	}

	@Override
	public ExprType getType() {
		return ExprType.NUMERIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		arg.accept(visitor);
		visitor.visitUnaryMinus(this);
	}
}

/**
 * Числовой нумерал.
 */
final class NumericLiteral extends Expr {
	private final String lexValue;

	NumericLiteral(View v, String lexValue) {
		super(v);
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
	 */
	public String getLexValue() {
		return lexValue;
	}

	@Override
	public ExprType getType() {
		return ExprType.NUMERIC;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		visitor.visitNumericLiteral(this);
	}
}

/**
 * Текстовый литерал.
 */
final class TextLiteral extends Expr {
	private final String lexValue;

	TextLiteral(View v, String lexValue) {
		super(v);
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
	 */
	public String getLexValue() {
		return lexValue;
	}

	@Override
	public ExprType getType() {
		return ExprType.TEXT;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		visitor.visitTextLiteral(this);
	}

}

/**
 * Ссылка на колонку таблицы.
 */
final class FieldRef extends Expr {
	private final String tableNameOrAlias;
	private final String columnName;
	private Column column = null;

	public FieldRef(View v, String tableNameOrAlias, String columnName)
			throws ParseException {
		super(v);
		if (columnName == null)
			throw new IllegalArgumentException();
		this.tableNameOrAlias = tableNameOrAlias;
		this.columnName = columnName;

	}

	/**
	 * Имя или алиас таблицы.
	 */
	public String getTableNameOrAlias() {
		return tableNameOrAlias;
	}

	/**
	 * Имя колонки.
	 */
	public String getColumnName() {
		return columnName;
	}

	@Override
	public ExprType getType() {
		if (column != null) {
			if (column instanceof IntegerColumn
					|| column instanceof FloatingColumn)
				return ExprType.NUMERIC;
			if (column instanceof StringColumn)
				return ExprType.TEXT;
			if (column instanceof BooleanColumn)
				return ExprType.BIT;
			if (column instanceof DateTimeColumn)
				return ExprType.DATE;
			if (column instanceof BinaryColumn)
				return ExprType.BLOB;
			// This should not happen unless we introduced new types in Celesta
			throw new IllegalStateException();
		}
		return ExprType.UNDEFINED;
	}

	/**
	 * Возвращает столбец, на который указывает ссылка.
	 */
	public Column getColumn() {
		return column;
	}

	/**
	 * Устанавливает столбец ссылки.
	 */
	public void setColumn(Column column) {
		this.column = column;
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		visitor.visitFieldRef(this);
	}
}
