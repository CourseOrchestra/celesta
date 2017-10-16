package ru.curs.celesta.score;

/**
 * Created by ioann on 10.07.2017.
 */
public final class Count extends Aggregate {
  @Override
  public ViewColumnMeta getMeta() {
    return new ViewColumnMeta(ViewColumnType.INT);
  }

  @Override
  void accept(ExprVisitor visitor) throws ParseException {
    visitor.visitCount(this);
  }

}