package ru.curs.celesta.score;

/**
 * Interface that defines a versioning possibility for a meta entity in the DB.
 *
 * @author ioann
 * @since 2017-06-13
 */
public interface VersionedElement {

  /**
   * Name of the system field containing version of the entry.
   */
  String REC_VERSION = "recversion";


  /**
   * Whether the table is versioned (WITH VERSION CHECK).
   *
   * @return
   */
  boolean isVersioned();

  /**
   * Returns a description of <em>recversion</em> field.
   *
   * @return
   */
  IntegerColumn getRecVersionField();

}
