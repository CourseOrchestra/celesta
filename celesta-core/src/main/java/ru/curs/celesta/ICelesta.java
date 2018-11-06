package ru.curs.celesta;

import ru.curs.celesta.dbutils.ILoggingManager;
import ru.curs.celesta.dbutils.IPermissionManager;
import ru.curs.celesta.dbutils.IProfiler;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.Properties;

/**
 * Interface of Celesta instance.
 */
public interface ICelesta extends AutoCloseable {

    /**
     * Returns a {@link TriggerDispatcher} of this celesta instance.
     * @return a trigger dispatcher of this celesta instance.
     */
    TriggerDispatcher getTriggerDispatcher();

    /**
     * Возвращает метаданные Celesta (описание таблиц).
     */
    Score getScore();

    /**
     * Возвращает свойства, с которыми была инициализирована Челеста. Внимание:
     * данный объект имеет смысл использовать только на чтение, динамическое
     * изменение этих свойств не приводит ни к чему.
     */
    Properties getSetupProperties();

    /**
     * Returns a {@link IPermissionManager} of this celesta instance.
     * @return a permission manager of this celesta instance.
     */
    IPermissionManager getPermissionManager();

    /**
     * Returns a {@link ILoggingManager} of this celesta instance.
     * @return a logging manager of this celesta instance.
     */
    ILoggingManager getLoggingManager();

    /**
     * Returns a {@link ConnectionPool} of this celesta instance.
     * @return a connection poll of this celesta instance.
     */
    ConnectionPool getConnectionPool();

    /**
     * Returns a {@link IProfiler} of this celesta instance.
     * @return a profiler of this celesta instance.
     */
    IProfiler getProfiler();

    /**
     * Returns a {@link DBAdaptor} of this celesta instance.
     * @return a db adaptor of this celesta instance.
     */
    DBAdaptor getDBAdaptor();

}
