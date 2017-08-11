package ru.curs.celesta.score;

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
}
