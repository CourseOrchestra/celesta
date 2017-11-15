package ru.curs.celesta.score;

import java.sql.Timestamp;

/**
 * Created by ioann on 10.08.2017.
 */
public class Parameter extends NamedElement {

  private final String celestaType;
  private final ViewColumnType type;

  public Parameter(String name, ViewColumnType type) throws ParseException {
    super(name);
    this.type = type;
    celestaType = type.getCelestaType();
  }

  public ViewColumnType getType() {
    return type;
  }

  public Class getJavaClass() {
    switch (type) {
      case BIT:  return Boolean.class;
      case DATE: return Timestamp.class;
      case TEXT: return String.class;
      case REAL: return Double.class;
      case INT:  return Integer.class;
      default:
        throw new RuntimeException("Type is not defined or incorrect");
    }
  }
}
