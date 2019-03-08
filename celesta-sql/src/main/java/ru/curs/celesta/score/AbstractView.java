package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for all view data elements.
 *
 * @author ioann
 * @since 2017-06-08
 */
public abstract class AbstractView extends DataGrainElement {

  boolean distinct;
  final Map<String, Expr> columns = new LinkedHashMap<>();
  final Map<String, FieldRef> groupByColumns = new LinkedHashMap<>();
  private final Map<String, TableRef> tables = new LinkedHashMap<>();
  static final Map<Class<? extends Expr>, Function<Expr, Column>> EXPR_CLASSES_AND_COLUMN_EXTRACTORS = new HashMap<>();

  static {
    EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(Count.class, (Expr frExpr) -> null);

    EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(FieldRef.class, (Expr frExpr) -> {
      FieldRef fr = (FieldRef) frExpr;
      return fr.getColumn();
    });
    EXPR_CLASSES_AND_COLUMN_EXTRACTORS.put(Sum.class, (Expr sumExpr) -> {
      Sum sum = (Sum) sumExpr;
      if (sum.term instanceof BinaryTermOp) {
        return null;
      }
      FieldRef fr = (FieldRef) sum.term;
      return fr.getColumn();
    });
  }

  public AbstractView(GrainPart grainPart, String name) throws ParseException {
    super(grainPart, name);
  }

  abstract String viewType();

  /**
   * Sets where condition for SQL query.
   *
   * @param whereCondition  where condition.
   * @throws ParseException  if expression type is incorrect.
   */
  abstract void setWhereCondition(Expr whereCondition) throws ParseException;

  /**
   * Writes SELECT script to the stream.
   *
   * @param bw  output stream
   * @param gen  SQL generator (visitor)
   * @throws IOException  if writing to stream fails
   */
  public void selectScript(final PrintWriter bw, SQLGenerator gen) throws IOException {
    BWWrapper bww = new BWWrapper();

    writeSelectPart(bw, gen, bww);
    writeFromPart(bw, gen);
    writeWherePart(bw, gen);
    writeGroupByPart(bw, gen);
  }

  /**
   * Writes SELECT part to the stream.
   *
   * @param bw  output stream
   * @param gen  SQL generator (visitor)
   * @param bww  line break wrapper
   * @throws IOException  if writing to stream fails
   */
  void writeSelectPart(final PrintWriter bw, SQLGenerator gen, BWWrapper bww) throws IOException {
    bww.append("  select ", bw);
    if (distinct) {
      bww.append("distinct ", bw);
    }

    boolean cont = false;
    for (Map.Entry<String, Expr> e : columns.entrySet()) {
      if (cont) {
        bww.append(", ", bw);
      }
      String st = gen.generateSQL(e.getValue()) + " as ";
      if (gen.quoteNames()) {
        st = st + "\"" + e.getKey() + "\"";
      } else {
        st = st + e.getKey();
      }
      bww.append(st, bw);
      cont = true;
    }
    bw.println();
  }

  /**
   * Writes FROM part to the stream.
   *
   * @param bw  output stream
   * @param gen  SQL generator (visitor)
   * @throws IOException  if writing to stream fails
   */
  void writeFromPart(final PrintWriter bw, SQLGenerator gen) throws IOException {
    bw.write("  from ");
    boolean cont = false;
    for (TableRef tRef : getTables().values()) {
      if (cont) {
        bw.println();
        bw.printf("    %s ", tRef.getJoinType().toString());
        bw.write("join ");
      }
      bw.write(gen.tableName(tRef));
      if (cont) {
        bw.write(" on ");
        bw.write(gen.generateSQL(tRef.getOnExpr()));
      }
      cont = true;
    }
  }

  /**
   * Writes WHERE part to the stream.
   *
   * @param bw  output stream
   * @param gen  SQL generator (visitor)
   * @throws IOException  if writing to stream fails
   */
  void writeWherePart(final PrintWriter bw, SQLGenerator gen) throws IOException {
  }

  /**
   * Writes GROUP BY part to the stream.
   *
   * @param bw  output stream
   * @param gen  SQL generator (visitor)
   * @throws IOException  if writing to stream fails
   */
  void writeGroupByPart(final PrintWriter bw, SQLGenerator gen) throws IOException {
    if (!groupByColumns.isEmpty()) {
      bw.println();
      bw.write(" group by ");

      int countOfProcessed = 0;
      for (Expr field : groupByColumns.values()) {
        bw.write(gen.generateSQL(field));

        if (++countOfProcessed != groupByColumns.size()) {
          bw.write(", ");
        }
      }

    }
  }

  /**
   * Adds a column to the view.
   *
   * @param alias  column alias.
   * @param expr  column expression.
   * @throws ParseException  Non-unique alias name or some other semantic error
   */
  void addColumn(String alias, Expr expr) throws ParseException {
    if (expr == null) {
      throw new IllegalArgumentException();
    }

    if (alias == null || alias.isEmpty()) {
      throw new ParseException(String.format("%s '%s' contains a column with undefined alias.", viewType(), getName()));
    }
    alias = getGrain().getScore().getIdentifierParser().parse(alias);
    if (columns.containsKey(alias)) {
      throw new ParseException(String.format(
          "%s '%s' already contains column with name or alias '%s'. Use unique aliases for %s columns.",
          viewType(), getName(), alias, viewType()));
    }

    columns.put(alias, expr);
  }


  /**
   * Adds a column to the "GROUP BY" clause of the view.
   *
   * @param fr  Column expression.
   * @throws ParseException  Non-unique alias name, missing column in selection or some other semantic error
   */
  void addGroupByColumn(FieldRef fr) throws ParseException {
    if (fr == null) {
      throw new IllegalArgumentException();
    }

    String alias = fr.getColumnName();

    if (groupByColumns.containsKey(alias)) {
      throw new ParseException(String.format(
          "Duplicate column '%s' in GROUP BY expression for %s '%s.%s'.",
          alias, viewType(), getGrain().getName(), getName()));
    }

    Expr existedColumn = columns.get(fr.getColumnName());

    if (existedColumn == null) {
      throw new ParseException("Couldn't resolve column ref " + fr.getColumnName());
    }

    if (existedColumn.getClass().equals(FieldRef.class)) {
      FieldRef existedColumnFr = (FieldRef) existedColumn;
      fr.setTableNameOrAlias(existedColumnFr.getTableNameOrAlias());
      fr.setColumnName(existedColumnFr.getColumnName());
      fr.setColumn(existedColumnFr.getColumn());
    }

    groupByColumns.put(alias, fr);
  }

  /**
   * Adds a table reference to the view.
   *
   * @param ref  Table reference.
   * @throws ParseException  Non-unique alias or some other semantic error.
   */
  void addFromTableRef(TableRef ref) throws ParseException {
    if (ref == null) {
      throw new IllegalArgumentException();
    }

    String alias = ref.getAlias();
    if (alias == null || alias.isEmpty()) {
      throw new ParseException(String.format("%s '%s' contains a table with undefined alias.", viewType(), getName()));
    }
    if (getTables().containsKey(alias)) {
      throw new ParseException(String.format(
          "%s, '%s' already contains table with name or alias '%s'. Use unique aliases for %s tables.",
          viewType(), getName(), alias, viewType()));
    }

    getTables().put(alias, ref);

    Expr onCondition = ref.getOnExpr();
    if (onCondition != null) {
      onCondition.resolveFieldRefs(new ArrayList<>(getTables().values()));
      onCondition.validateTypes();
    }
  }

  /**
   * Finalizes view parsing, resolving field references and checking expression types.
   *
   * @throws ParseException  Error on types checking or reference resolving.
   */
  abstract void finalizeParsing() throws ParseException;

  void finalizeColumnsParsing() throws ParseException {
    List<TableRef> t = new ArrayList<>(getTables().values());
    for (Expr e : columns.values()) {
      e.resolveFieldRefs(t);
      e.validateTypes();
    }
  }


  void finalizeGroupByParsing() throws ParseException {
    //Check that columns which were not used for aggregation are mentioned in GROUP BY clause
    Set<String> aggregateAliases = columns.entrySet().stream()
        .filter(e -> e.getValue() instanceof Aggregate)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    if (!((aggregateAliases.isEmpty() || aggregateAliases.size() == columns.size())
        && groupByColumns.isEmpty())) {

      //Iterate by columns which are not aggregates and throw an exception
      // if at least one of them is absent from groupByColumns
      boolean hasErrorOpt = columns.entrySet().stream()
          .anyMatch(e -> !(e.getValue() instanceof Aggregate) && !groupByColumns.containsKey(e.getKey()));
      if (hasErrorOpt) {
        throw new ParseException(String.format("%s '%s.%s' contains a column(s) "
                + "which was not specified in aggregate function and GROUP BY expression.",
            viewType(), getGrain().getName(), getName()));
      }
    }
  }

  /**
   * Whether DISTINCT keyword was used in the view query.
   *
   * @return
   */
  boolean isDistinct() {
    return distinct;
  }

  /**
   * Sets the use of keyword DISTINCT in the view query.
   *
   * @param distinct  Whether the query has the form of SELECT DISTINCT.
   */
  void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  /**
   * Returns a map of columns of the view.
   *
   * @return
   */
  public abstract Map<String, ? extends ColumnMeta> getColumns();


  public Map<String, TableRef> getTables() {
    return tables;
  }

  /**
   * Returns column index by column name.
   */
  @Override
  public int getColumnIndex(String name) {
    int i = -1;
    for (String c : getColumns().keySet()) {
      i++;
      if (c.equals(name)) {
        return i;
      }
    }
    return i;
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

  /**
   * Returns column reference by column name.
   *
   * @param colName  Column name.
   * @return
   */
  public Column getColumnRef(String colName) {
    Expr expr = columns.get(colName);
    return EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
  }

  /**
   * Wrapper for automatic line-breaks.
   */
  class BWWrapper {
    private static final int LINE_SIZE = 80;
    private static final String PADDING = "    ";
    private int l = 0;

    private void append(String s, PrintWriter bw) throws IOException {
      bw.write(s);
      l += s.length();
      if (l >= LINE_SIZE) {
        bw.println();
        bw.write(PADDING);
        l = PADDING.length();
      }
    }
  }

}
