package ru.curs.celesta;

import ru.curs.celesta.dbutils.ILoggingManager;
import ru.curs.celesta.dbutils.IPermissionManager;
import ru.curs.celesta.dbutils.IProfiler;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.Properties;

public interface ICelesta {

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

    IPermissionManager getPermissionManager();

    ILoggingManager getLoggingManager();

    ConnectionPool getConnectionPool();

    IProfiler getProfiler();

    DBAdaptor getDBAdaptor();

}
