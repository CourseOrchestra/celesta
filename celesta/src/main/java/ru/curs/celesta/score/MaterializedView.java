package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by ioann on 08.06.2017.
 */
public class MaterializedView extends AbstractView implements TableElement {

  @FunctionalInterface
  private interface MatColFabricFunction {
    Column apply(MaterializedView mView, Column colRef, String alias) throws ParseException;
  }

  private static final Map<Class<? extends Column>, MatColFabricFunction> COL_CLASSES_AND_FABRIC_FUNCS = new HashMap<>();

  static {
    COL_CLASSES_AND_FABRIC_FUNCS.put(IntegerColumn.class, (mView, colRef, alias) -> new IntegerColumn(mView, alias));
    COL_CLASSES_AND_FABRIC_FUNCS.put(FloatingColumn.class, (mView, colRef, alias) -> new FloatingColumn(mView, alias));
    COL_CLASSES_AND_FABRIC_FUNCS.put(BooleanColumn.class, (mView, colRef, alias) -> new BooleanColumn(mView, alias));
    COL_CLASSES_AND_FABRIC_FUNCS.put(BinaryColumn.class, (mView, colRef, alias) -> new BinaryColumn(mView, alias));
    COL_CLASSES_AND_FABRIC_FUNCS.put(DateTimeColumn.class, (mView, colRef, alias) -> new DateTimeColumn(mView, alias));
    COL_CLASSES_AND_FABRIC_FUNCS.put(StringColumn.class, (mView, colRef, alias) -> {
      StringColumn result = new StringColumn(mView, alias);
      StringColumn strColRef = (StringColumn) colRef;
      result.setLength(String.valueOf(strColRef.getLength()));
      return result;
    });
  }

  private static final Map<Class<? extends Expr>, Function<Expr, Column>> EXPR_CLASSES_AND_COLUMN_EXTRACTORS = new HashMap<>();

  static {
    EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(FieldRef.class, (Expr frExpr) -> {
      FieldRef fr = (FieldRef) frExpr;
      return fr.getColumn();
    });
    EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(Sum.class, (Expr sumExpr) -> {
      Sum sum = (Sum) sumExpr;
      FieldRef fr = (FieldRef) sum.term;
      return fr.getColumn();
    });
  }

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
      Column colRef = null;

      final MatColFabricFunction matColFabricFunction;

      if (expr instanceof Count)
        matColFabricFunction = COL_CLASSES_AND_FABRIC_FUNCS.get(IntegerColumn.class);
      else {
        colRef = EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
        matColFabricFunction = COL_CLASSES_AND_FABRIC_FUNCS.get(colRef.getClass());
      }

      if (matColFabricFunction == null) {
        throw new ParseException(String.format(
            "Unsupported type '%s' of column '%s' in materialized view %s was found",
            type, alias, getName()));
      } else {
        col = matColFabricFunction.apply(this, colRef, alias);
      }


      if (!(expr instanceof Aggregate)) {
        pk.addElement(col);
        col.setNullableAndDefault(false, null);
      }

    }
  }

  @Override
  void finalizeGroupByParsing() throws ParseException {
    super.finalizeGroupByParsing();

    for (String alias : groupByColumns.keySet()) {
      Column colRef = ((FieldRef) columns.get(alias)).getColumn();
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


  public List<String> getColumnRefNames() {
    List<String> result = new ArrayList<>();

    for (Map.Entry<String, Expr> entry : columns.entrySet()) {
      Expr expr = entry.getValue();

      if (!(expr instanceof Count)) {
        Column colRef = EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
        result.add(colRef.getName());
      }
    }

    return result;
  }

  public Map<String, Expr> getAggregateColumns() {
    return columns.entrySet().stream()
        .filter(e -> e.getValue() instanceof Aggregate)
        .collect(Collectors.toMap(
            Map.Entry::getKey, Map.Entry::getValue,
            (o, o2) -> {
              throw new IllegalStateException(String.format("Duplicate key %s", o));
            }, LinkedHashMap::new));
  }

  @Override
  public Column getColumn(String colName) throws ParseException {
    Column result = realColumns.get(colName);
    if (result == null)
      throw new ParseException(
          String.format("Column '%s' not found in materialized view '%s.%s'", colName, getGrain().getName(), getName()));
    return result;
  }

  public Column getColumnRef(String colName) {
    Expr expr = columns.get(colName);

    if (expr instanceof Count) {
      return null;
    }
    return EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
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

  public TableRef getRefTable() {
    return getTables().values().stream().findFirst().get();
  }

  public boolean isGroupByColumn(String alias) {
    return groupByColumns.containsKey(alias);
  }

  public String getSelectPartOfScript() {
    try {
      SQLGenerator gen = new SQLGenerator();
      StringWriter sw = new StringWriter();
      BufferedWriter bw = new BufferedWriter(sw);
      BWWrapper bww = new BWWrapper();

      writeSelectPart(bw, gen, bww);
      bw.flush();
      return sw.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getGroupByPartOfScript() {
    try {
      SQLGenerator gen = new SQLGenerator();
      StringWriter sw = new StringWriter();
      BufferedWriter bw = new BufferedWriter(sw);

      writeGroupByPart(bw, gen);
      bw.flush();
      return sw.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
