package ru.curs.celesta.dbutils.filter;

import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Created by ioann on 01.06.2017.
 */
public final class In {

  final private Map<FieldsLookup, WhereTermsMaker> lookupWhereTermMap = new LinkedHashMap<>();

  public In(FieldsLookup lookup, WhereTermsMaker otherWhereTermMaker) {
    lookupWhereTermMap.put(lookup, otherWhereTermMaker);
  }

  public void addLookup(FieldsLookup lookup, WhereTermsMaker otherWhereTermMaker) {
    lookupWhereTermMap.put(lookup, otherWhereTermMaker);
  }

  public Collection<FieldsLookup> getLookups() {
    return lookupWhereTermMap.keySet();
  }

  public Collection<WhereTermsMaker> getOtherWhereTermMakers() {
    return lookupWhereTermMap.values();
  }

  public Map<FieldsLookup, WhereTermsMaker> getLookupWhereTermMap() {
    return lookupWhereTermMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof In)) return false;

    In other = (In) o;
    return this.lookupWhereTermMap.keySet().equals(other.lookupWhereTermMap.keySet());
  }

}


