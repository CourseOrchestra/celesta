package ru.curs.celesta.score;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * View object in metadata.
 */
public class View extends AbstractView {

  Map<String, ViewColumnMeta> columnTypes = null;
  Expr whereCondition;

  View(GrainPart grainPart, String name) throws ParseException {
    super(grainPart, name);
    getGrain().addElement(this);
  }

  public View(GrainPart grainPart, String name, String sql) throws ParseException {
    this(grainPart, name);
    StringReader sr = new StringReader(sql);
    CelestaParser parser = new CelestaParser(sr);
    try {
      try {
        parser.select(this);
      } finally {
        sr.close();
      }
      finalizeParsing();
    } catch (ParseException e) {
      delete();
      throw e;
    }
  }

  @Override
  String viewType() {
    return "view";
  }

  @Override
  void finalizeParsing() throws ParseException {
    finalizeColumnsParsing();
    finalizeWhereConditionParsing();
    finalizeGroupByParsing();
  }

  void finalizeWhereConditionParsing() throws ParseException {
    List<TableRef> t = new ArrayList<>(getTables().values());
    if (whereCondition != null) {
      whereCondition.resolveFieldRefs(t);
      whereCondition.validateTypes();
    }
  }


  @Override
  void setWhereCondition(Expr whereCondition) throws ParseException {
    if (whereCondition != null) {
      List<TableRef> t = new ArrayList<>(getTables().values());
      whereCondition.resolveFieldRefs(t);
      whereCondition.assertType(ViewColumnType.LOGIC);
    }
    this.whereCondition = whereCondition;
  }

  @Override
  public Map<String, ViewColumnMeta> getColumns() {
    if (columnTypes == null) {
      columnTypes = new LinkedHashMap<>();
      for (Map.Entry<String, Expr> e : columns.entrySet()) {
        columnTypes.put(e.getKey(), e.getValue().getMeta());
      }
    }
    return columnTypes;
  }

  /**
   * Creates CREATE VIEW script in different SQL dialects by using 'visitor' pattern.
   *
   * @param bw  stream that the saving is performed into
   * @param gen  generator-visitor
   * @throws IOException  error on writing to stream
   */
  public void createViewScript(PrintWriter bw, SQLGenerator gen) throws IOException {
    bw.println(gen.preamble(this));
    selectScript(bw, gen);
  }

  @Override
  final void writeWherePart(PrintWriter bw, SQLGenerator gen) throws IOException {
    if (whereCondition != null) {
      bw.println();
      bw.write("  where ");
      bw.write(gen.generateSQL(whereCondition));
    }
  }

}
