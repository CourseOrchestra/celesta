package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.PlainIdentifierParser;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Parameter element of score.
 *
 * @author ioann
 * @since 2017-08-10
 */
public final class Parameter extends NamedElement {

  private final String celestaType;
  private final ViewColumnType type;

  public Parameter(String name, ViewColumnType type) throws ParseException {
    super(name, new PlainIdentifierParser());
    this.type = type;
    celestaType = type.getCelestaType();
  }

  /**
   * Returns parameter type.
   *
   * @return
   */
  public ViewColumnType getType() {
    return type;
  }

  /**
   * Returns java type of parameter.
   * @return
   */
  public Class<?> getJavaClass() {
    switch (type) {
      case BIT:  return Boolean.class;
      case DATE: return Timestamp.class;
      case TEXT: return String.class;
      case REAL: return Double.class;
      case DECIMAL: return BigDecimal.class;
      case INT:  return Integer.class;
      default:
        throw new RuntimeException("Type is not defined or incorrect");
    }
  }

}
