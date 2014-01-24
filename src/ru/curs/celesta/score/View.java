package ru.curs.celesta.score;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Объект-представление в метаданных.
 */
public class View extends NamedElement {

	private Grain grain;

	private boolean distinct;

	private Map<String, Expr> columns = new LinkedHashMap<>();

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

}

/** Скалярное выражение SQL. */
abstract class Expr {
}

/**
 * Выражение в скобках.
 */
final class ParenthesizedExpr extends TerminalExpr {
	private final TerminalExpr parenthesized;

	public ParenthesizedExpr(TerminalExpr parenthesized) {
		this.parenthesized = parenthesized;
	}

	/**
	 * Возвращает выражение, заключенное в скобки.
	 */
	public TerminalExpr getParenthesized() {
		return parenthesized;
	}
}

/**
 * Логическое выражение.
 */
abstract class LogicalExpr extends Expr {

}

/**
 * Отношение (>=, <=, <>, =, <, >).
 */
final class Relop extends LogicalExpr {
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

	private final Expr left;
	private final Expr right;
	private final int relop;

	public Relop(Expr left, Expr right, int relop) {
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

}

/**
 * ... IN (..., ..., ...).
 */
final class In extends LogicalExpr {
	// TODO
}

/**
 * BETWEEN.
 */
final class Between extends LogicalExpr {
	// TODO
}

/**
 * NOT.
 */
final class UnaryLogicalOp extends LogicalExpr {
	// TODO
}

/**
 * AND/OR.
 */
final class BinaryLogicalOp extends LogicalExpr {
	public static final int AND = 0;
	public static final int OR = 1;
	// TODO
}

/**
 * Терм.
 */
abstract class TerminalExpr extends Expr {
}

/**
 * +, -, *, /.
 */
final class BinaryOp extends NumericExpr {
	public static final int PLUS = 0;
	public static final int MINUS = 0;
	public static final int TIMES = 0;
	public static final int OVER = 0;
}

/**
 * Унарный минус.
 */
final class UnaryOp extends NumericExpr {
	private final TerminalExpr arg;// TODO

	public UnaryOp(TerminalExpr arg) {
		this.arg = arg;
	}

	public TerminalExpr getExpr() {
		return arg;
	}
}

/**
 * Числовой нумерал.
 */
final class NumericLiteral extends NumericExpr {
	private final String lexValue;

	NumericLiteral(String lexValue) {
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
	 */
	public String getLexValue() {
		return lexValue;
	}
}

/**
 * Числовое выражение.
 */
abstract class NumericExpr extends TerminalExpr {
	// TODO
}

/**
 * Текстовое выражение.
 */
abstract class TextExpr extends TerminalExpr {

}

/**
 * Текстовый литерал.
 */
class TextLiteral extends TextExpr {
	private final String lexValue;

	TextLiteral(String lexValue) {
		this.lexValue = lexValue;
	}

	/**
	 * Возвращает лексическое значение.
	 */
	public String getLexValue() {
		return lexValue;
	}

}

/**
 * Конкатенация текстовых значений.
 */
class TextConcat extends TextExpr {
	// TODO
}

/**
 * Ссылка на колонку таблицы.
 */
final class FieldRef extends TerminalExpr {
	private final String grainName;
	private final String tableNameOrAlias;
	private final String columnName;

	public FieldRef(String grainName, String tableNameOrAlias, String columnName) {
		this.grainName = grainName;
		this.tableNameOrAlias = tableNameOrAlias;
		this.columnName = columnName;
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

}
