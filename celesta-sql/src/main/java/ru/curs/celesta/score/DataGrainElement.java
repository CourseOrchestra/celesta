package ru.curs.celesta.score;

/**
 * Parent class for grain elements containing data:
 * <ul>
 *   <li>Tables</li>
 *   <li>Views</li>
 *   <li>Materialized views</li>
 * </ul>
 *
 * @author ioann
 * @since 2017-08-09
 */
public abstract class DataGrainElement extends GrainElement implements HasColumns {
  public DataGrainElement(GrainPart gp, String name) throws ParseException {
    super(gp, name);
  }

  /**
   * Deletes element.
   *
   * @throws ParseException  on a try to change the system grain
   */
  public void delete() throws ParseException {
    getGrain().removeElement(this);
  }
}
