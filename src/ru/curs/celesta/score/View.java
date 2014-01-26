package ru.curs.celesta.score;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Объект-представление в метаданных.
 */
public class View extends NamedElement {

	private Grain grain;

	private boolean distinct;

	private final Map<String, Expr> columns = new LinkedHashMap<>();
	private final Map<String, Expr> unmodifiableColumns = Collections
			.unmodifiableMap(columns);

	public View(Grain grain, String name) throws ParseException {
		super(name);
		if (grain == null)
			throw new IllegalArgumentException();
		this.grain = grain;
		grain.addView(this);
	}

	/**
	 * Возвращает гранулу, к которой относится представление.
	 */
	public Grain getGrain() {
		return grain;
	}

	/**
	 * Использовано ли слово DISTINCT в запросе представления.
	 */
	public boolean isDistinct() {
		return distinct;
	}

	/**
	 * Устанавливает использование слова DISTINCT в запросе представления.
	 * 
	 * @param distinct
	 *            Если запрос имеет вид SELECT DISTINCT.
	 */
	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}

	/**
	 * Добавляет колонку к представлению.
	 * 
	 * @param alias
	 *            Алиас колонки.
	 * @param expr
	 *            Выражение колонки.
	 * @throws ParseException
	 *             Неуникальное имя алиаса или иная семантическая ошибка
	 */
	void addColumn(String alias, Expr expr) throws ParseException {
		if (expr == null)
			throw new IllegalArgumentException();

		if (alias == null || alias.isEmpty())
			throw new ParseException(String.format(
					"View '%s' contains a column with undefined alias.",
					getName()));
		if (columns.containsKey(alias))
			throw new ParseException(
					String.format(
							"View '%s' already contains column with name or alias '%s'. Use unique aliases for view columns.",
							getName(), alias));

		columns.put(alias, expr);
	}

	/**
	 * Возвращает перечень столбцов представления.
	 */
	public Map<String, Expr> getColumns() {
		return unmodifiableColumns;
	}

}

/**
 * Тип выражения.
 */
enum ExprType {
	LOGIC, NUMERIC, TEXT, OTHER, UNDEFINED
}

/** Скалярное выражение SQL. */
abstract class Expr {
	private final View v;

	Expr(View v) {
		this.v = v;
	}

	/**
	 * Возвращает Celesta-SQL представление выражения.
	 */
	public abstract String getCSQL();

	/**
	 * Возвращает тип выражения.
	 */
	public abstract ExprType getType();

	final View getView() {
		return v;
	}
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
	public String getCSQL() {
		return "(" + parenthesized.getCSQL() + ")";
	}

	@Override
	public ExprType getType() {
		return parenthesized.getType();
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

	private static final String[] OPS = { " > ", " < ", " >= ", " <= ", " <> ",
			" = ", " LIKE " };

	private final Expr left;
	private final Expr right;
	private final int relop;

	public Relop(View v, Expr left, Expr right, int relop) {
		super(v);
		if (relop < 0 || relop >= OPS.length)
			throw new IllegalArgumentException();
		// TODO: сравнивать можно только однотипные термы
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
	public String getCSQL() {
		return left.getCSQL() + OPS[relop] + right.getCSQL();
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
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
		// TODO: все операнды должны быть однотипны
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
	public String getCSQL() {
		StringBuilder result = new StringBuilder(left.getCSQL());
		result.append(" IN (");
		boolean needComma = false;
		for (Expr operand : operands) {
			if (needComma)
				result.append(", ");
			result.append(operand.getCSQL());
			needComma = true;
		}
		result.append(")");
		return result.toString();
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
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
		// TODO: все операнды должны быть однотипны
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
	public String getCSQL() {
		return String.format("%s BETWEEN %s AND %s", left.getCSQL(),
				right1.getCSQL(), right2.getCSQL());
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}

}

/**
 * IS NULL.
 */
final class IsNull extends Expr {
	private final Expr expr;

	IsNull(View v, Expr expr) {
		super(v);
		// TODO это должен быть терм
		this.expr = expr;
	}

	/**
	 * Выражение, от которого берётся IS NULL.
	 */
	public Expr getExpr() {
		return expr;
	}

	@Override
	public String getCSQL() {
		return expr.getCSQL() + "IS NULL";
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}
}

/**
 * NOT.
 */
final class NotExpr extends Expr {
	private final Expr expr;

	NotExpr(View v, Expr expr) {
		super(v);
		// TODO это должен быть LOGIC
		this.expr = expr;
	}

	/**
	 * Выражение, от которого берётся NOT.
	 */
	public Expr getExpr() {
		return expr;
	}

	@Override
	public String getCSQL() {
		return "NOT " + expr.getCSQL();
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
	}
}

/**
 * AND/OR.
 */
final class BinaryLogicalOp extends Expr {
	public static final int AND = 0;
	public static final int OR = 1;
	private static final String[] OPS = { " AND ", " OR " };

	private final int operator;
	private final List<Expr> operands;

	BinaryLogicalOp(View v, int operator, List<Expr> operands) {
		super(v);
		if (operator < 0 || operator >= OPS.length)
			throw new IllegalArgumentException();
		// TODO: все операнды должны быть логическими
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
	public String getCSQL() {
		StringBuilder result = new StringBuilder();
		boolean needOp = false;
		for (Expr operand : operands) {
			if (needOp)
				result.append(OPS[operator]);
			result.append(operand.getCSQL());
			needOp = true;
		}
		return result.toString();
	}

	@Override
	public ExprType getType() {
		return ExprType.LOGIC;
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
	private static final String[] OPS = { " + ", " - ", " * ", " / ", " || " };

	private final int operator;
	private final List<Expr> operands;

	BinaryTermOp(View v, int operator, List<Expr> operands) {
		super(v);
		if (operator < 0 || operator >= OPS.length)
			throw new IllegalArgumentException();
		// TODO: для CONCAT все должны быть TEXT, для остальных -- NUMERIC
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
	public String getCSQL() {
		StringBuilder result = new StringBuilder();
		boolean needOp = false;
		for (Expr operand : operands) {
			if (needOp)
				result.append(OPS[operator]);
			result.append(operand.getCSQL());
			needOp = true;
		}
		return result.toString();
	}

	@Override
	public ExprType getType() {
		return operator == CONCAT ? ExprType.TEXT : ExprType.NUMERIC;
	}
}

/**
 * Унарный минус.
 */
final class UnaryMinus extends Expr {
	private final Expr arg;

	public UnaryMinus(View v, Expr arg) {
		super(v);
		// TODO операнд должен быть NUMERIC
		this.arg = arg;
	}

	public Expr getExpr() {
		return arg;
	}

	@Override
	public String getCSQL() {
		return "-" + arg.getCSQL();
	}

	@Override
	public ExprType getType() {
		return ExprType.NUMERIC;
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
	public String getCSQL() {
		return lexValue;
	}

	@Override
	public ExprType getType() {
		return ExprType.NUMERIC;
	}
}

/**
 * Текстовый литерал.
 */
class TextLiteral extends Expr {
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
	public String getCSQL() {
		return lexValue;
	}

	@Override
	public ExprType getType() {
		return ExprType.TEXT;
	}

}

/**
 * Ссылка на колонку таблицы.
 */
final class FieldRef extends Expr {
	private final String grainName;
	private final String tableNameOrAlias;
	private final String columnName;
	private Column column;

	public FieldRef(View v, String grainName, String tableNameOrAlias,
			String columnName) throws ParseException {
		super(v);
		if (columnName == null)
			throw new IllegalArgumentException();
		if (grainName != null && tableNameOrAlias == null)
			throw new IllegalArgumentException();
		this.grainName = grainName;
		this.tableNameOrAlias = tableNameOrAlias;
		this.columnName = columnName;
		if (grainName != null) {
			Grain g = v.getGrain().getScore().getGrain(grainName);
			Table t = g.getTable(tableNameOrAlias);
			column = t.getColumn(columnName);
		}
	}

	/**
	 * Имя гранулы.
	 */
	public String getGrainName() {
		return grainName;
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
	public String getCSQL() {
		StringBuilder result = new StringBuilder();
		if (grainName != null) {
			result.append(grainName);
			result.append(".");
		}
		if (tableNameOrAlias != null) {
			result.append(tableNameOrAlias);
			result.append(".");
		}
		result.append(columnName);
		return result.toString();
	}

	@Override
	public ExprType getType() {
		if (column != null) {
			if (column instanceof IntegerColumn
					|| column instanceof FloatingColumn)
				return ExprType.NUMERIC;
			if (column instanceof StringColumn)
				return ExprType.TEXT;
			return ExprType.OTHER;
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
}
