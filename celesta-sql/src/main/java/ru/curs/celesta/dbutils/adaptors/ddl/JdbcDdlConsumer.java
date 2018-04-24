package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.dbutils.jdbc.SqlUtils;

import java.sql.Connection;

public class JdbcDdlConsumer implements DdlConsumer {
    @Override
    public void consume(Connection conn, String sql)  {
        SqlUtils.executeUpdate(conn, sql);
    }
}
