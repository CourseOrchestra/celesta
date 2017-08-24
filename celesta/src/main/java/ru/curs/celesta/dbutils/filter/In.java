package ru.curs.celesta.dbutils.filter;

import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;


/**
 * Created by ioann on 01.06.2017.
 */
public final class In {

  final private FieldsLookup lookup;
  final private WhereTermsMaker otherWhereTermMaker;

  public In(FieldsLookup lookup, WhereTermsMaker otherWhereTermMaker) {
    this.lookup = lookup;
    this.otherWhereTermMaker = otherWhereTermMaker;
  }

  public FieldsLookup getLookup() {
    return lookup;
  }

  public WhereTermsMaker getOtherWhereTermMaker() {
    return otherWhereTermMaker;
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


