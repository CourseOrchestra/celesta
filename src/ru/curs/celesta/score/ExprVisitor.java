package ru.curs.celesta.score;

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

	void visitBooleanLiteral(BooleanLiteral expr) throws ParseException {

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
		for (TableRef tRef : tables) {
			if (fr.getTableNameOrAlias() != null && fr.getTableNameOrAlias().equals(tRef.getAlias())) {
				fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
				foundCounter++;
			} else
				if (fr.getTableNameOrAlias() == null && tRef.getTable().getColumns().containsKey(fr.getColumnName())) {
				fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
				foundCounter++;
			}
		}
		if (foundCounter == 0)
			throw new ParseException(String.format("Cannot resolve field reference '%s'", fr.getCSQL()));
		if (foundCounter > 1)
			throw new ParseException(String.format("Ambiguous field reference '%s'", fr.getCSQL()));

	}
}

/**
 * Проверяльщик типов.
 */
final class TypeChecker extends ExprVisitor {
	void visitBetween(Between expr) throws ParseException {
		ViewColumnType t = expr.getLeft().getType();
		// Сравнивать можно не все типы.
		if (t == ViewColumnType.DATE || t == ViewColumnType.NUMERIC || t == ViewColumnType.TEXT) {
			// все операнды должны быть однотипны
			expr.getRight1().assertType(t);
			expr.getRight2().assertType(t);
		} else {
			throw new ParseException(
					String.format("Wrong expression '%s': type %s cannot be used in ...BETWEEN...AND... expression.",
							expr.getCSQL(), t.toString()));
		}
	}

	void visitBinaryTermOp(BinaryTermOp expr) throws ParseException {
		// для CONCAT все операнды должны быть TEXT, для остальных -- NUMERIC
		ViewColumnType t = expr.getOperator() == BinaryTermOp.CONCAT ? ViewColumnType.TEXT : ViewColumnType.NUMERIC;
		for (Expr e : expr.getOperands())
			e.assertType(t);
	}

	void visitIn(In expr) throws ParseException {
		ViewColumnType t = expr.getLeft().getType();
		// Сравнивать можно не все типы.
		if (t == ViewColumnType.DATE || t == ViewColumnType.NUMERIC || t == ViewColumnType.TEXT) {
			// все операнды должны быть однотипны
			for (Expr operand : expr.getOperands()) {
				operand.assertType(t);
			}
		} else {
			throw new ParseException(
					String.format("Wrong expression '%s': type %s cannot be used in ...IN(...) expression.",
							expr.getCSQL(), t.toString()));
		}

	}

	void visitRelop(Relop expr) throws ParseException {
		ViewColumnType t = expr.getLeft().getType();
		// Сравнивать можно не все типы.
		if (t == ViewColumnType.DATE || t == ViewColumnType.NUMERIC || t == ViewColumnType.TEXT) {
			// сравнивать можно только однотипные термы
			expr.getRight().assertType(t);
			// при этом like действует только на строковых термах
			if (expr.getRelop() == Relop.LIKE)
				expr.getLeft().assertType(ViewColumnType.TEXT);
		} else if (t == ViewColumnType.BIT && expr.getRelop() == Relop.EQ) {
			if (expr.getRight().getType() != ViewColumnType.BIT) {
				throw new ParseException(String.format(
						"Wrong expression '%s': "
								+ "BIT field can be compared with another BIT field or TRUE/FALSE constants only.",
						expr.getCSQL()));
			}
		} else {
			throw new ParseException(String.format("Wrong expression '%s': type %s cannot be used in comparisions.",
					expr.getCSQL(), t.toString()));
		}
	}

	void visitUnaryMinus(UnaryMinus expr) throws ParseException {
		// операнд должен быть NUMERIC
		expr.getExpr().assertType(ViewColumnType.NUMERIC);
	}

	@Override
	void visitNotExpr(NotExpr expr) throws ParseException {
		expr.getExpr().assertType(ViewColumnType.LOGIC);
	}

}