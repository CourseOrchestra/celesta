package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Created by ioann on 08.06.2017.
 */
public class MaterializedView extends AbstractView {

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
    //2. создать триггеры
  }


  @Override
  public void delete() throws ParseException {
    getGrain().removeMaterializedView(this);
  }

  /**
   * Генератор CelestaSQL.
   */
  private class CelestaSQLGen extends SQLGenerator {

  }
}
