package ru.curs.celesta.score;

/**
 * Created by ioann on 09.08.2017.
 */
public abstract class DataGrainElement extends GrainElement {
  public DataGrainElement(Grain g, String name) throws ParseException {
    super(g, name);
  }

  /**
   * Удаляет элемент.
   *
   * @throws ParseException при попытке изменить системную гранулу
   */
  public void delete() throws ParseException {
    getGrain().removeElement(this);
  }
}
