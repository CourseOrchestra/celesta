package ru.curs.celesta.score;

public abstract class ExprVisitor {
	abstract void visitBetween(Expr e);

	abstract void visitBinaryLogicalOp(Expr e);

	abstract void visitBinaryTermOp(Expr e);

	abstract void visitFieldRef(Expr e);

	abstract void visitIn(Expr e);

	abstract void visitIsNull(Expr e);

	abstract void visitNotExpr(Expr e);

	abstract void visitNumericLiteral(Expr e);

	abstract void visitParenthesizedExpr(Expr e);

	abstract void visitRelop(Expr e);

	abstract void visitTextLiteral(Expr e);

	abstract void visitUnaryMinus(Expr e);
}
