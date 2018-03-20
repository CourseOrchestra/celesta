package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;

import java.sql.Connection;

@FunctionalInterface
public interface DdlConsumer {
    void consume(Connection conn, String sql) throws CelestaException;
}
