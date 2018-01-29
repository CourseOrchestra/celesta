package ru.curs.celesta.score;

/**
 * Created by ioann on 09.08.2017.
 */
public abstract class DataGrainElement extends GrainElement implements HasColumns {
  public DataGrainElement(GrainPart gp, String name) throws ParseException {
    super(gp, name);
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
