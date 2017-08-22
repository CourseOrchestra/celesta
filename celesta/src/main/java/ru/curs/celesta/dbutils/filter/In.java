package ru.curs.celesta.dbutils.filter;

import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.score.Table;


/**
 * Created by ioann on 01.06.2017.
 */
public final class In {

  final private FieldsLookup lookup;
  final private WhereTerm otherwhereTerm;

  public In(FieldsLookup lookup, WhereTerm otherwhereTerm) {
    this.lookup = lookup;
    this.otherwhereTerm = otherwhereTerm;
  }

  public FieldsLookup getLookup() {
    return lookup;
  }

  public WhereTerm getOtherwhereTerm() {
    return otherwhereTerm;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof In)) return false;

    In other = (In) o;
    return this.lookup.equals(other.lookup);
  }

}


