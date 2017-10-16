package ru.curs.celesta.score;

/**
 * Created by ioann on 10.07.2017.
 */
public final class Max extends Aggregate {
  Expr term;

  Max(Expr term) {
    this.term = term;
  }

  @Override
  void accept(ExprVisitor visitor) throws ParseException {
    term.accept(visitor);
    visitor.visitMax(this);
  }

  @Override
  public ViewColumnMeta getMeta() {
    return term.getMeta();
  }
}
