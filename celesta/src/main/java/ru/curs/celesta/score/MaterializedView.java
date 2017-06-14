package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ioann on 08.06.2017.
 */
public class MaterializedView extends AbstractView implements TableElement {

  private final NamedElementHolder<Column> realColumns = new NamedElementHolder<Column>() {
    @Override
    protected String getErrorMsg(String name) {
      return String.format("Column '%s' defined more than once in table '%s'.", name, getName());
    }
  };

  private final NamedElementHolder<Column> pk = new NamedElementHolder<Column>() {
    @Override
    protected String getErrorMsg(String name) {
      return String.format("Column '%s' defined more than once for primary key in table '%s'.", name, getName());
    }
  };

  public MaterializedView(Grain g, String name) throws ParseException {
    super(g, name);
    g.addMaterializedView(this);
  }

  @Override
  String viewType() {
    return "materialized view";
  }

  @Override
  void finalizeParsing() throws ParseException {

    //Присутствие хотя бы одного агрегатного столбца - обязательное условие
    Optional aggregate = columns.entrySet().stream()
        .filter(e -> e.getValue() instanceof Aggregate)
        .findFirst();

    if (!aggregate.isPresent()) {
      throw new ParseException(String.format("%s %s.%s must have at least one aggregate column"
      , viewType(), getGrain().getName(), getName()));
    }

    finalizeColumnsParsing();
    finalizeGroupByParsing();
  }

  @Override
  void finalizeColumnsParsing() throws ParseException {
    super.finalizeColumnsParsing();

    for (Map.Entry<String, Expr> entry : columns.entrySet()) {
      String alias = entry.getKey();
      Expr expr = entry.getValue();

      ViewColumnMeta vcm = expr.getMeta();

      String type = vcm.getCelestaType();

      final Column col;

      if (IntegerColumn.CELESTA_TYPE.equals(type)) {
        col = new IntegerColumn(this, alias);
      } else if (FloatingColumn.CELESTA_TYPE.equals(type)) {
        col = new FloatingColumn(this, alias);
      } else if (BooleanColumn.CELESTA_TYPE.equals(type)) {
        col = new BooleanColumn(this, alias);
      } else if (StringColumn.VARCHAR.equals(type)) {
        col = new StringColumn(this, alias);
      } else if (BinaryColumn.CELESTA_TYPE.equals(type)) {
        col = new BinaryColumn(this, alias);
      } else if (DateTimeColumn.CELESTA_TYPE.equals(type)) {
        col = new DateTimeColumn(this, alias);
      } else {
        throw new ParseException(String.format(
            "Unsupported type '%s' of column '%s' in materialized view %s was found",
            type, alias, getName()));
      }

      if (!(expr instanceof Aggregate)) {
        pk.addElement(col);
      }

    }
  }

  @Override
  void save(BufferedWriter bw) throws IOException {
    SQLGenerator gen = new CelestaSQLGen();
    Grain.writeCelestaDoc(this, bw);
    bw.write(";");
    bw.newLine();
    bw.newLine();
  }

  @Override
  public void delete() throws ParseException {
    getGrain().removeMaterializedView(this);
  }

  @Override
  void setWhereCondition(Expr whereCondition) throws ParseException {
    throw new ParseException(String.format("Exception while parsing materialized view %s.%s " +
        "Materialized views doesn't support where condition.", getGrain().getName(), getName()));
  }

  @Override
  public Map<String, Column> getColumns() {
    return realColumns.getElements();
  }

  @Override
  public Column getColumn(String colName) throws ParseException {
    Column result = realColumns.get(colName);
    if (result == null)
      throw new ParseException(
          String.format("Column '%s' not found in materialized view '%s.%s'", colName, getGrain().getName(), getName()));
    return result;
  }

  @Override
  public void addColumn(Column column) throws ParseException {
    if (column.getParentTable() != this)
      throw new IllegalArgumentException();
    getGrain().modify();
    realColumns.addElement(column);
  }

  @Override
  public synchronized void removeColumn(Column column) throws ParseException {
    // Составную часть первичного ключа нельзя удалить
    if (pk.contains(column))
      throw new ParseException(
          String.format(YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "a primary key. Change primary key first.",
              getGrain().getName(), getName(), column.getName()));
    // Составную часть индекса нельзя удалить
    for (Index ind : getGrain().getIndices().values())
      if (ind.getColumns().containsValue(column))
        throw new ParseException(String.format(
            YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "an index. Drop or change relevant index first.",
            getGrain().getName(), getName(), column.getName()));

    getGrain().modify();
    realColumns.remove(column);
  }

  @Override
  public boolean hasPrimeKey() {
    return !pk.getElements().isEmpty();
  }

  @Override
  public String getPkConstraintName() {
    return limitName("pk_" + getName());
  }

  @Override
  public Map<String, Column> getPrimaryKey() {
    return pk.getElements();
  }


  /**
   * Генератор CelestaSQL.
   */
  private class CelestaSQLGen extends SQLGenerator {

  }
}
