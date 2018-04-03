package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;

public interface ILoggingManager {

    void log(Cursor c, Action a) throws CelestaException;
}
