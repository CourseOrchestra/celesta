package ru.curs.celesta.score;

/**
 * Created by ioann on 10.07.2017.
 */
public final class Sum extends Aggregate {
  Expr term;

  Sum(Expr term) {
    this.term = term;
  }

  @Override
  public ViewColumnMeta getMeta() {
    return term.getMeta();
  }


  @Override
  void accept(ExprVisitor visitor) throws ParseException {
    term.accept(visitor);
    visitor.visitSum(this);
  }
}
