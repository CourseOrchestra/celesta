package ru.curs.celesta.dbutils.adaptors.ddl;

import java.sql.Connection;

@FunctionalInterface
public interface DdlConsumer {
    void consume(Connection conn, String sql);
}
