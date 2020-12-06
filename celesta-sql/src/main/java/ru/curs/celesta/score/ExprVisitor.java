package ru.curs.celesta.score;

import java.util.*;

/**
 * Syntax tree visitor for procedure implementation of types validation, code
 * generation etc. (see Visitor pattern).
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

  void visitParameterRef(ParameterRef expr) throws ParseException {

  }

  void visitIn(In expr) throws ParseException {
  }

  void visitIsNull(IsNull expr) throws ParseException {
  }

  void visitNotExpr(NotExpr expr) throws ParseException {
  }

  void visitRealLiteral(RealLiteral expr) throws ParseException {
  }

  void visitIntegerLiteral(IntegerLiteral expr) throws ParseException {
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

  void visitGetDate(GetDate expr) throws ParseException {
  }

  void visitCount(Count expr) throws ParseException {
  }

  void visitSum(Sum expr) throws ParseException {
  }

  void visitMax(Max expr) throws ParseException {
  }

  void visitMin(Min expr) throws ParseException {
  }

  void visitUpper(Upper expr) throws ParseException {
  }

  void visitLower(Lower expr) throws ParseException {
  }

}

/**
 * Class for reference resolving in the context of existing tables.
 */
final class FieldResolver extends ExprVisitor {

  private final List<TableRef> tables;

  public FieldResolver(List<TableRef> tables) {
    this.tables = tables;
  }

  @Override
  void visitFieldRef(FieldRef fr) throws ParseException {

    if (fr.getColumn() != null) {
      return;
    }
    int foundCounter = 0;
    for (TableRef tRef : tables) {
      if (fr.getTableNameOrAlias() != null && fr.getTableNameOrAlias().equals(tRef.getAlias())) {
        fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
        foundCounter++;
      } else if (fr.getTableNameOrAlias() == null && tRef.getTable().getColumns().containsKey(fr.getColumnName())) {
        fr.setColumn(tRef.getTable().getColumn(fr.getColumnName()));
        foundCounter++;
      }
    }
    if (foundCounter == 0) {
      throw new ParseException(String.format("Cannot resolve field reference '%s'", fr.getCSQL()));
    }
    if (foundCounter > 1) {
      throw new ParseException(String.format("Ambiguous field reference '%s'", fr.getCSQL()));
    }
  }
}

/**
 * Class for reference resolving in the context of existing parameters.
 */
final class ParameterResolver extends ExprVisitor {
  private final Map<String, Parameter> parameters;
  private final ParameterResolverResult result;

  public ParameterResolver(Map<String, Parameter> parameters) {
    this.parameters = parameters;
    this.result =  new ParameterResolverResult();
    this.result.getUnusedParameters().addAll(parameters.keySet());
  }

  @Override
  void visitParameterRef(ParameterRef pr) throws ParseException {
    if (pr.getParameter() != null) {
      return;
    }

    Parameter parameter = parameters.get(pr.getName());

    if (parameter == null) {
      throw new ParseException(
          String.format("Cannot resolve parameter '%s'", pr.getCSQL())
      );
    }

    pr.setParameter(parameter);
    result.getUnusedParameters().remove(parameter.getName());
    result.getParametersWithUsageOrder().add(parameter.getName());
  }

  public ParameterResolverResult getResult() {
    return result;
  }
}

final class ParameterResolverResult {
  private Set<String> unusedParameters = new HashSet<>();
  private List<String> parametersWithUsageOrder = new ArrayList<>();

  public Set<String> getUnusedParameters() {
    return unusedParameters;
  }

  public List<String> getParametersWithUsageOrder() {
    return parametersWithUsageOrder;
  }
}

/**
 * Types checker.
 */
final class TypeChecker extends ExprVisitor {
  void visitBetween(Between expr) throws ParseException {
    final ViewColumnType t = expr.getLeft().getMeta().getColumnType();
    // It's not needed to compare all types.
    if (t == ViewColumnType.DATE || t == ViewColumnType.REAL || t == ViewColumnType.INT
        || t == ViewColumnType.TEXT) {
      // all operands should be of the same type
      expr.getRight1().assertType(t);
      expr.getRight2().assertType(t);
    } else {
      throw new ParseException(
          String.format("Wrong expression '%s': type %s cannot be used in ...BETWEEN...AND... expression.",
              expr.getCSQL(), t.toString()));
    }
  }

  void visitBinaryTermOp(BinaryTermOp expr) throws ParseException {
    // for CONCAT all operands should be of TEXT, for the others -- NUMERIC
    final ViewColumnType t = expr.getOperator() == BinaryTermOp.CONCAT ? ViewColumnType.TEXT : ViewColumnType.REAL;
    for (Expr e : expr.getOperands()) {
      e.assertType(t);
    }
  }

  void visitIn(In expr) throws ParseException {
    final ViewColumnType t = expr.getLeft().getMeta().getColumnType();
    // It's not needed to compare all types.
    if (t == ViewColumnType.DATE || t == ViewColumnType.REAL || t == ViewColumnType.INT
        || t == ViewColumnType.TEXT) {
      // all operands should be of the same type
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
    final ViewColumnType t = expr.getLeft().getMeta().getColumnType();
    // It's not needed to compare all types.
    if (t == ViewColumnType.DATE || t == ViewColumnType.REAL || t == ViewColumnType.DECIMAL
            || t == ViewColumnType.INT || t == ViewColumnType.TEXT) {
      // only terms of the same type may be compared
      expr.getRight().assertType(t);
      // wherein 'like' is valid only for string terms
      if (expr.getRelop() == Relop.LIKE) {
        expr.getLeft().assertType(ViewColumnType.TEXT);
      }
    } else if (t == ViewColumnType.BIT && expr.getRelop() == Relop.EQ) {
      if (expr.getRight().getMeta().getColumnType() != ViewColumnType.BIT) {
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
    // operand should be of NUMERIC
    expr.getExpr().assertType(ViewColumnType.REAL);
  }

  @Override
  void visitNotExpr(NotExpr expr) throws ParseException {
    expr.getExpr().assertType(ViewColumnType.LOGIC);
  }

  @Override
  void visitSum(Sum expr) throws ParseException {
    expr.term.assertType(ViewColumnType.REAL);
  }


  @Override
  void visitUpper(Upper expr) throws ParseException {
    expr.getArg().assertType(ViewColumnType.TEXT);
  }

  @Override
  void visitLower(Lower expr) throws ParseException {
    expr.getArg().assertType(ViewColumnType.TEXT);
  }
}
