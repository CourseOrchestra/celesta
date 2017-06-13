package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by ioann on 08.06.2017.
 */
public class MaterializedView extends AbstractView implements TableElement {

  private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
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
  void save(BufferedWriter bw) throws IOException {
    SQLGenerator gen = new CelestaSQLGen();
    Grain.writeCelestaDoc(this, bw);
    bw.write(";");
    bw.newLine();
    bw.newLine();
  }

  /**
   * Создаёт скрипт CREATE... в различных диалектах SQL, используя паттерн
   * visitor.
   *
   * @param bw  поток, в который происходит сохранение.
   * @param gen генератор-visitor
   * @throws IOException ошибка записи в поток
   */
  @Override
  public void createViewScript(BufferedWriter bw, SQLGenerator gen) throws IOException {
    //1. Создать таблицу
    createTable(bw, gen);
    //2. создать триггеры
  }

  private void createTable(BufferedWriter bw, SQLGenerator gen) throws IOException {

  }

  @Override
  public void delete() throws ParseException {
    getGrain().removeMaterializedView(this);
  }

  @Override
  public Map<String, Column> getColumns() {
    return columns.getElements();
  }

  @Override
  public Column getColumn(String colName) throws ParseException {
    Column result = columns.get(colName);
    if (result == null)
      throw new ParseException(
          String.format("Column '%s' not found in materialized view '%s.%s'", colName, getGrain().getName(), getName()));
    return result;
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
    columns.remove(column);
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
