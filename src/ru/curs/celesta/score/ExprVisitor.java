package ru.curs.celesta.score;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Посетитель синтаксического дерева для реализации процедур контроля типов,
 * кодогенерации и проч. (см. паттерн Visitor).
 * 
 */
public abstract class ExprVisitor {
	void visitBetween(Between expr) throws ParseException {
	}

	void visitBinaryLogicalOp(BinaryLogicalOp expr) throws ParseException {
	}

	void visitBinaryTermOp(BinaryTermOp expr) throws ParseException {
	}

	void visitFieldRef(FieldRef expr) throws ParseException {
	}

	void visitIn(In expr) throws ParseException {
	}

	void visitIsNull(IsNull expr) throws ParseException {
	}

	void visitNotExpr(NotExpr expr) throws ParseException {
	}

	void visitNumericLiteral(NumericLiteral expr) throws ParseException {
	}

	void visitParenthesizedExpr(ParenthesizedExpr expr) throws ParseException {
	}

	void visitRelop(Relop expr) throws ParseException {
	}

	void visitTextLiteral(TextLiteral expr) throws ParseException {
	}

	void visitUnaryMinus(UnaryMinus expr) throws ParseException {
	}
}

/**
 * Класс разрешения ссылок в контексте имеющихся таблиц.
 */
final class FieldResolver extends ExprVisitor {

	private final List<TableRef> tables;

	public FieldResolver(List<TableRef> tables) {
		this.tables = tables;
	}

	@Override
	void visitFieldRef(FieldRef fr) throws ParseException {

		if (fr.getColumn() != null)
			return;
		int foundCounter = 0;
		for (TableRef tRef : tables)
			if (fr.getGrainName() == null) {
				if (fr.getTableNameOrAlias() != null
						&& fr.getTableNameOrAlias().equals(tRef.getAlias())) {
					fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
					foundCounter++;
				} else if (fr.getTableNameOrAlias() == null
						&& tRef.getTable().getColumns()
								.containsKey(fr.getColumnName())) {
					fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
					foundCounter++;
				}
			} else {
				if (fr.getGrainName().equals(
						tRef.getTable().getGrain().getName())
						&& fr.getTableNameOrAlias().equals(
								tRef.getTable().getName())) {
					fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
					foundCounter++;
				}
			}
		if (foundCounter == 0)
			throw new ParseException(String.format(
					"Cannot resolve field reference '%s'", fr.getCSQL()));
		if (foundCounter > 1)
			throw new ParseException(String.format(
					"Ambiguous field reference '%s'", fr.getCSQL()));

	}
}

/**
 * Проверяльщик типов.
 */
final class TypeChecker extends ExprVisitor {
	void visitBetween(Between expr) throws ParseException {
		ExprType t = expr.getLeft().getType();
		// Сравнивать можно не все типы.
		if (t == ExprType.DATE || t == ExprType.NUMERIC || t == ExprType.TEXT) {
			// все операнды должны быть однотипны
			expr.getRight1().assertType(t);
			expr.getRight2().assertType(t);
		} else {
			throw new ParseException(
					String.format(
							"Wrong expression '%s': type %s cannot be used in ...BETWEEN...AND... expression.",
							expr.getCSQL(), t.toString()));
		}
	}

	void visitBinaryTermOp(BinaryTermOp expr) throws ParseException {
		// для CONCAT все операнды должны быть TEXT, для остальных -- NUMERIC
		ExprType t = expr.getOperator() == BinaryTermOp.CONCAT ? ExprType.TEXT
				: ExprType.NUMERIC;
		for (Expr e : expr.getOperands())
			e.assertType(t);
	}

	void visitIn(In expr) throws ParseException {
		ExprType t = expr.getLeft().getType();
		// Сравнивать можно не все типы.
		if (t == ExprType.DATE || t == ExprType.NUMERIC || t == ExprType.TEXT) {
			// все операнды должны быть однотипны
			for (Expr operand : expr.getOperands()) {
				operand.assertType(t);
			}
		} else {
			throw new ParseException(
					String.format(
							"Wrong expression '%s': type %s cannot be used in ...IN(...) expression.",
							expr.getCSQL(), t.toString()));
		}

	}

	void visitRelop(Relop expr) throws ParseException {
		ExprType t = expr.getLeft().getType();
		// Сравнивать можно не все типы.
		if (t == ExprType.DATE || t == ExprType.NUMERIC || t == ExprType.TEXT) {
			// сравнивать можно только однотипные термы
			expr.getRight().assertType(t);
			// при этом like действует только на строковых термах
			if (expr.getRelop() == Relop.LIKE)
				expr.getLeft().assertType(ExprType.TEXT);
		} else {
			throw new ParseException(
					String.format(
							"Wrong expression '%s': type %s cannot be used in comparisions.",
							expr.getCSQL(), t.toString()));
		}
	}

	void visitUnaryMinus(UnaryMinus expr) throws ParseException {
		// операнд должен быть NUMERIC
		expr.getExpr().assertType(ExprType.NUMERIC);
	}
}

/**
 * Класс-генератор SQL-выражений.
 */
class SQLGenerator extends ExprVisitor {
	private Deque<String> stack = new LinkedList<>();

	public String generateSQL(Expr e) {
		try {
			e.accept(this);
		} catch (ParseException e1) {
			// This should never ever happen
			throw new RuntimeException(e1);
		}
		return stack.pop();
	}

	String concat() {
		return " || ";
	}

	final void visitBetween(Between expr) throws ParseException {
		String right2 = stack.pop();
		String right1 = stack.pop();
		String left = stack.pop();
		stack.push(String.format("%s BETWEEN %s AND %s", left, right1, right2));
	}

	final void visitBinaryLogicalOp(BinaryLogicalOp expr) throws ParseException {
		StringBuilder result = new StringBuilder();
		String op = BinaryLogicalOp.OPS[expr.getOperator()];
		LinkedList<String> operands = new LinkedList<>();
		for (int i = 0; i < expr.getOperands().size(); i++)
			operands.push(stack.pop());
		boolean needOp = false;
		for (String operand : operands) {
			if (needOp)
				result.append(op);
			result.append(operand);
			needOp = true;
		}
		stack.push(result.toString());
	}

	final void visitBinaryTermOp(BinaryTermOp expr) throws ParseException {
		StringBuilder result = new StringBuilder();
		String op = expr.getOperator() == BinaryTermOp.CONCAT ? concat()
				: BinaryTermOp.OPS[expr.getOperator()];
		LinkedList<String> operands = new LinkedList<>();
		for (int i = 0; i < expr.getOperands().size(); i++)
			operands.push(stack.pop());

		boolean needOp = false;
		for (String operand : operands) {
			if (needOp)
				result.append(op);
			result.append(operand);
			needOp = true;
		}
		stack.push(result.toString());
	}

	void visitFieldRef(FieldRef expr) throws ParseException {
		StringBuilder result = new StringBuilder();
		if (expr.getGrainName() != null) {
			result.append(expr.getGrainName());
			result.append(".");
		}
		if (expr.getTableNameOrAlias() != null) {
			result.append(expr.getTableNameOrAlias());
			result.append(".");
		}
		result.append(expr.getColumnName());
		stack.push(result.toString());
	}

	void visitIn(In expr) throws ParseException {
		StringBuilder result = new StringBuilder();
		LinkedList<String> operands = new LinkedList<>();
		for (int i = 0; i < expr.getOperands().size(); i++)
			operands.push(stack.pop());
		result.append(stack.pop());
		result.append(" IN (");
		boolean needComma = false;
		for (String operand : operands) {
			if (needComma)
				result.append(", ");
			result.append(operand);
			needComma = true;
		}
		result.append(")");
		stack.push(result.toString());
	}

	void visitIsNull(IsNull expr) throws ParseException {
		stack.push(stack.pop() + " IS NULL");
	}

	void visitNotExpr(NotExpr expr) throws ParseException {
		stack.push("NOT " + expr.getCSQL());
	}

	void visitNumericLiteral(NumericLiteral expr) throws ParseException {
		stack.push(expr.getLexValue());
	}

	void visitParenthesizedExpr(ParenthesizedExpr expr) throws ParseException {
		stack.push("(" + stack.pop() + ")");
	}

	void visitRelop(Relop expr) throws ParseException {
		String right = stack.pop();
		String left = stack.pop();
		stack.push(left + Relop.OPS[expr.getRelop()] + right);
	}

	void visitTextLiteral(TextLiteral expr) throws ParseException {
		stack.push(expr.getLexValue());
	}

	void visitUnaryMinus(UnaryMinus expr) throws ParseException {
		stack.push("-" + stack.pop());
	}
}