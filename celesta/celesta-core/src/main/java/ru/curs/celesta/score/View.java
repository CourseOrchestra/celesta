package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Объект-представление в метаданных.
 */
public class View extends AbstractView {

  Map<String, ViewColumnMeta> columnTypes = null;
  private String queryString;
  Expr whereCondition;

  View(Grain grain, String name) throws ParseException {
    super(grain, name);
    grain.addElement(this);
  }

  public View(Grain grain, String name, String sql) throws ParseException {
    this(grain, name);
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

  public Map<String, ViewColumnMeta> getColumns()  {
    if (columnTypes == null) {
      columnTypes = new LinkedHashMap<>();
      for (Map.Entry<String, Expr> e : columns.entrySet())
        columnTypes.put(e.getKey(), e.getValue().getMeta());
    }
    return columnTypes;
  }

  /**
   * Создаёт скрипт CREATE VIEW в различных диалектах SQL, используя паттерн
   * visitor.
   *
   * @param bw  поток, в который происходит сохранение.
   * @param gen генератор-visitor
   * @throws IOException ошибка записи в поток
   */
  public void createViewScript(BufferedWriter bw, SQLGenerator gen) throws IOException {
    bw.write(gen.preamble(this));
    bw.newLine();
    selectScript(bw, gen);
  }

  @Override
  void writeWherePart(BufferedWriter bw, SQLGenerator gen) throws IOException {
    if (whereCondition != null) {
      bw.newLine();
      bw.write("  where ");
      bw.write(gen.generateSQL(whereCondition));
    }
  }

  @Override
  void save(BufferedWriter bw) throws IOException {
    SQLGenerator gen = new CelestaSQLGen();
    Grain.writeCelestaDoc(this, bw);
    createViewScript(bw, gen);
    bw.write(";");
    bw.newLine();
    bw.newLine();
  }


  /**
   * Возвращает SQL-запрос на языке Celesta, на основании которого построено
   * представление.
   */
  public String getCelestaQueryString() {
    if (queryString != null)
      return queryString;
    StringWriter sw = new StringWriter();
    BufferedWriter bw = new BufferedWriter(sw);
    SQLGenerator gen = new CelestaSQLGen();
    try {
      selectScript(bw, gen);
      bw.flush();
    } catch (IOException e) {
      // This should never happen for in-memory streams
      throw new RuntimeException(e);
    }

    queryString = sw.toString();
    return queryString;
  }

}