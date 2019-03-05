package ru.curs.celesta.dbutils;

import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.AbstractScore;

public class DbUpdaterAccessor {

    public static AbstractScore getScore(DbUpdater<?> dbUpdater) {
        return dbUpdater.score;
    }

    public static ConnectionPool getConnectionPool(DbUpdater<?> dbUpdater) {
        return dbUpdater.connectionPool;
    }

    public static DBAdaptor getDbAdaptor(DbUpdater<?> dbUpdater) {
        return dbUpdater.dbAdaptor;
    }
}
