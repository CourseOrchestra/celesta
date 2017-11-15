package ru.curs.celesta.score;

/**
 * Created by ioann on 10.07.2017.
 */
public final class Min  extends Aggregate {
  Expr term;

  Min(Expr term) {
    this.term = term;
  }

  @Override
  void accept(ExprVisitor visitor) throws ParseException {
    term.accept(visitor);
    visitor.visitMin(this);
  }

  @Override
  public ViewColumnMeta getMeta() {
    return term.getMeta();
  }
}
