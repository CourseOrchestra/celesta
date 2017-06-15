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
      Column colRef;

      FieldRef fr = null;

      if (expr instanceof FieldRef) {
        fr = (FieldRef) expr;
      } else if (expr instanceof Max) {
        fr = (FieldRef) ((Max) expr).term;
      } else if (expr instanceof Min) {
        fr = (FieldRef) ((Min) expr).term;
      } else if (expr instanceof Sum) {
        fr = (FieldRef) ((Sum) expr).term;
      }

      colRef = fr.getColumn();

      if (colRef instanceof IntegerColumn) {
        col = new IntegerColumn(this, alias);
      } else if (colRef instanceof FloatingColumn) {
        col = new FloatingColumn(this, alias);
      } else if (colRef instanceof BooleanColumn) {
        col = new BooleanColumn(this, alias);
      } else if (colRef instanceof StringColumn) {
        col = new StringColumn(this, alias);
        StringColumn strColRef = (StringColumn) colRef;
        ((StringColumn)col).setLength(String.valueOf(strColRef.getLength()));
      } else if (colRef instanceof BinaryColumn) {
        col = new BinaryColumn(this, alias);
      } else if (colRef instanceof DateTimeColumn) {
        col = new DateTimeColumn(this, alias);
      } else {
        throw new ParseException(String.format(
            "Unsupported type '%s' of column '%s' in materialized view %s was found",
            type, alias, getName()));
      }


      if (!(expr instanceof Aggregate)) {
        pk.addElement(col);
        col.setNullable(false);
      }

    }
  }

  @Override
  void finalizeGroupByParsing() throws ParseException {
    super.finalizeGroupByParsing();

    for (String alias : groupByColumns.keySet()) {
      Column colRef = ((FieldRef)columns.get(alias)).getColumn();
      if (colRef.isNullable()) {
        throw new ParseException(String.format(
            "Nullable column %s was found in GROUP BY expression for %s '%s.%s'.",
            alias, viewType(), getGrain().getName(), getName())
        );
      }
    }
  }

  @Override
  void save(BufferedWriter bw) throws IOException {
    SQLGenerator gen = new CelestaSQLGen();
    Grain.writeCelestaDoc(this, bw);
    bw.write(gen.preamble(this));
    bw.newLine();
    selectScript(bw, gen);
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

    @Override
    protected String preamble(AbstractView view) {
      return String.format("create materialized view %s as", viewName(view));
    }

    @Override
    protected String viewName(AbstractView v) {
      return getName();
    }

    @Override
    protected String tableName(TableRef tRef) {
      Table t = tRef.getTable();
      if (t.getGrain() == getGrain()) {
        return String.format("%s as %s", t.getName(), tRef.getAlias());
      } else {
        return String.format("%s.%s as %s", t.getGrain().getName(), t.getName(), tRef.getAlias());
      }
    }

    @Override
    protected boolean quoteNames() {
      return false;
    }
  }
}
