package ru.curs.celesta.score;

/**
 * Интерфейс, определяющий возможность версионирования метасущности в бд
 *
 * Created by ioann on 13.06.2017.
 */
public interface VersionedElement {

  /**
   * Имя системного поля, содержащего версию записи.
   */
  String REC_VERSION = "recversion";


  /**
   * Является ли таблица версионированной (WITH VERSION CHECK).
   */
  boolean isVersioned();

  /**
   * Возвращает описание поля recversion.
   */
  IntegerColumn getRecVersionField();

}
