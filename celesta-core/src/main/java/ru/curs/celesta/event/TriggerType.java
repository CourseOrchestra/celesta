package ru.curs.celesta.event;

/**
 * Trigger type enumeration.
 *
 * @author ioann
 * @since 2017-05-31
 */
public enum TriggerType {
  /**
   * PRE_DELETE type.
   */
  PRE_DELETE,

  /**
   * POST_DELETE type.
   */
  POST_DELETE,

  /**
   * PRE_UPDATE type.
   */
  PRE_UPDATE,

  /**
   * POST_UPDATE type.
   */
  POST_UPDATE,

  /**
   * PRE_INSERT type.
   */
  PRE_INSERT,

  /**
   * POST_INSERT type.
   */
  POST_INSERT
}
