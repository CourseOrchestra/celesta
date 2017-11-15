package ru.curs.celesta.score;

import java.util.List;
import java.util.Map;
import java.util.Set;

import ru.curs.celesta.dbutils.QueryBuildingHelper;

/** Скалярное выражение SQL. */
public abstract class Expr {

	final void assertType(ViewColumnType t) throws ParseException {
		// INT and REAL are both numeric types, so they are comparable
		final ViewColumnType columnType = getMeta().getColumnType();
		if (t == ViewColumnType.INT || t == ViewColumnType.REAL) {
			if (!(columnType == ViewColumnType.INT || columnType == ViewColumnType.REAL))
				throw new ParseException(
						String.format("Expression '%s' is expected to be of numeric type, but it is %s", getCSQL(),
								getMeta().toString()));
		} else {
			if (columnType != t)
				throw new ParseException(String.format("Expression '%s' is expected to be of %s type, but it is %s",
						getCSQL(), t.toString(), getMeta().toString()));
		}
	}

	/**
	 * Возвращает Celesta-SQL представление выражения.
	 */
	public final String getCSQL() {
		SQLGenerator gen = new SQLGenerator();
		return gen.generateSQL(this);
	}

	/**
	 * Возвращает SQL-представление выражения в диалекте текущей БД.
	 * 
	 * @param dba
	 *            Адаптер БД.
	 */
	public final String getSQL(QueryBuildingHelper dba) {
		SQLGenerator gen = dba.getViewSQLGenerator();
		return gen.generateSQL(this);
	}

	/**
	 * Возвращает тип выражения.
	 */
	public abstract ViewColumnMeta getMeta();

	/**
	 * Разрешает ссылки на поля таблиц, используя контекст объявленных таблиц с
	 * их псевдонимами.
	 * 
	 * @param tables
	 *            перечень таблиц.
	 * @throws ParseException
	 *             в случае, если ссылка не может быть разрешена.
	 */
	final void resolveFieldRefs(List<TableRef> tables) throws ParseException {
		FieldResolver r = new FieldResolver(tables);
		accept(r);
	}

	/**
	 * Разрешает ссылки на поля таблиц, используя контекст текущего объекта
	 * партитуры.
	 * 
	 * @param ge
	 *            таблица или представление.
	 * @throws ParseException
	 *             в случае, если ссылка не может быть разрешена.
	 */
	public final void resolveFieldRefs(GrainElement ge) throws ParseException {
		FilterFieldResolver fr = new FilterFieldResolver(ge);
		accept(fr);
	}

	final ParameterResolverResult resolveParameterRefs(Map<String, Parameter> parameters) throws ParseException {
		ParameterResolver r = new ParameterResolver(parameters);
		accept(r);
		return r.getResult();
	}

	/**
	 * Проверяет типы всех входящих в выражение субвыражений.
	 * 
	 * @throws ParseException
	 *             в случае, если контроль типов не проходит.
	 */
	final void validateTypes() throws ParseException {
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
	abstract void accept(ExprVisitor visitor) throws ParseException;
}

/**
 * Выражение в скобках.
 */
final class ParenthesizedExpr extends Expr {
	private final Expr parenthesized;

	public ParenthesizedExpr(Expr parenthesized) {
		this.parenthesized = parenthesized;
	}

	/**
	 * Возвращает выражение, заключенное в скобки.
	 */
	public Expr getParenthesized() {
		return parenthesized;
	}

	@Override
	public ViewColumnMeta getMeta() {
		return parenthesized.getMeta();
	}

	@Override
	public void accept(ExprVisitor visitor) throws ParseException {
		parenthesized.accept(visitor);
		visitor.visitParenthesizedExpr(this);
	}
}

/**
 * Expression with logical value. Celestadoc and nullable is not actual.
 */
abstract class LogicValuedExpr extends Expr {
	private static final ViewColumnMeta META;

	static {
		META = new ViewColumnMeta(ViewColumnType.LOGIC);
		META.setNullable(false);
	}

	@Override
	public final ViewColumnMeta getMeta() {
		return META;
	}

}

/**
 * Отношение (>=, <=, <>, =, <, >).
 */
final class Relop extends LogicValuedExpr {
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
	public void accept(ExprVisitor visitor) throws ParseException {
		left.accept(visitor);
		right.accept(visitor);
		visitor.visitRelop(this);
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
		if (expr.getMeta().getColumnType() == ViewColumnType.LOGIC)
			throw new ParseException(String.format(
					"Expression '%s' is logical condition and cannot be an argument of IS NULL operator.", getCSQL()));
		this.expr = expr;
	}

	/**
	 * Выражение, от которого берётся IS NULL.
	 */
	public Expr getExpr() {
		return expr;
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
	 * Выражение, от которого берётся NOT.
	 */
	public Expr getExpr() {
		return expr;
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
final class BinaryLogicalOp extends LogicValuedExpr {
	public static final int AND = 0;
	public static final int OR = 1;
	public static final String[] OPS = { " AND ", " OR " };

	private final int operator;
	private final List<Expr> operands;

	BinaryLogicalOp(int operator, List<Expr> operands) throws ParseException {
		if (operator < 0 || operator >= OPS.length)
			throw new IllegalArgumentException();
		if (operands.isEmpty())
			throw new IllegalArgumentException();
		// все операнды должны быть логическими
		for (Expr e : operands)
			if (e.getMeta().getColumnType() != ViewColumnType.LOGIC)
				throw new ParseException(
						String.format("Expression '%s' is expected to be logical condition.", e.getCSQL()));
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

	private ViewColumnMeta meta;

	private final int operator;
	private final List<Expr> operands;

	BinaryTermOp(int operator, List<Expr> operands) {
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
	public ViewColumnMeta getMeta() {
		if (meta == null) {
			cases: switch (operator) {
			case CONCAT: // ||
				meta = new ViewColumnMeta(ViewColumnType.TEXT);
				break;
			case OVER: // /
				meta = new ViewColumnMeta(ViewColumnType.REAL);
				break;
			default: // +, -, *
				for (Expr o : operands) {
					if (o.getMeta().getColumnType() == ViewColumnType.REAL) {
						meta = new ViewColumnMeta(ViewColumnType.REAL);
						break cases;
					}
				}
				meta = new ViewColumnMeta(ViewColumnType.INT);
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

	public UnaryMinus(Expr arg) {
		this.arg = arg;
	}

	public Expr getExpr() {
		return arg;
	}

	@Override
	public ViewColumnMeta getMeta() {
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
	private ViewColumnMeta meta;

	Literal(ViewColumnType type) {
		this.type = type;
	}

	@Override
	public final ViewColumnMeta getMeta() {
		if (meta == null) {
			meta = new ViewColumnMeta(type);
			meta.setNullable(false);
		}
		return meta;
	}
}

/**
 * Числовой нумерал с плавающей точкой.
 */
final class RealLiteral extends Literal {
	private final String lexValue;

	RealLiteral(String lexValue) {
		super(ViewColumnType.REAL);
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
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
 * Числовой нумерал.
 */
final class IntegerLiteral extends Literal {
	private final String lexValue;

	IntegerLiteral(String lexValue) {
		super(ViewColumnType.INT);
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
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
 * Литерал TRUE или FALSE.
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
 * Текстовый литерал.
 */
final class TextLiteral extends Literal {
	private final String lexValue;

	TextLiteral(String lexValue) {
		super(ViewColumnType.TEXT);
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
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
	public ViewColumnMeta getMeta() {
		return new ViewColumnMeta(ViewColumnType.DATE);
	}

	@Override
	void accept(ExprVisitor visitor) throws ParseException {
		visitor.visitGetDate(this);
	}
}


abstract class Aggregate extends Expr {

}


/**
 * Ссылка на колонку таблицы.
 */
final class FieldRef extends Expr {
	private final String tableNameOrAlias;
	private final String columnName;
	private Column column = null;
	private ViewColumnMeta meta;

	public FieldRef(String tableNameOrAlias, String columnName) throws ParseException {
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
	public ViewColumnMeta getMeta() {
		if (meta == null) {
			if (column != null) {
				if (column instanceof IntegerColumn)
					meta = new ViewColumnMeta(ViewColumnType.INT);
				else if (column instanceof FloatingColumn)
					meta = new ViewColumnMeta(ViewColumnType.REAL);
				else if (column instanceof StringColumn) {
					StringColumn sc = (StringColumn) column;
					if (sc.isMax())
						meta = new ViewColumnMeta(ViewColumnType.TEXT);
					else
						meta = new ViewColumnMeta(ViewColumnType.TEXT, sc.getLength());
				} else if (column instanceof BooleanColumn)
					meta = new ViewColumnMeta(ViewColumnType.BIT);
				else if (column instanceof DateTimeColumn)
					meta = new ViewColumnMeta(ViewColumnType.DATE);
				else if (column instanceof BinaryColumn)
					meta = new ViewColumnMeta(ViewColumnType.BLOB);
				// This should not happen unless we introduced new types in
				// Celesta
				else {
					throw new IllegalStateException();
				}
				meta.setNullable(column.isNullable());
				meta.setCelestaDoc(column.getCelestaDoc());
			} else {
				return new ViewColumnMeta(ViewColumnType.UNDEFINED);
			}
		}
		return meta;
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

final class ParameterRef extends Expr {

	private Parameter parameter;
	private String name;
	private ViewColumnMeta meta;

	public ParameterRef(String name) {
		this.name = name;
	}

	@Override
	public ViewColumnMeta getMeta() {
		if (meta == null) {
			if (parameter != null) {
				meta = new ViewColumnMeta(parameter.getType());
			} else {
				meta = new ViewColumnMeta(ViewColumnType.UNDEFINED);
			}
		}
		return meta;
	}

	@Override
	void accept(ExprVisitor visitor) throws ParseException {
		visitor.visitParameterRef(this);
	}

	public Parameter getParameter() {
		return parameter;
	}

	public void setParameter(Parameter parameter) {
		this.parameter = parameter;
	}

	public String getName() {
		return name;
	}
}