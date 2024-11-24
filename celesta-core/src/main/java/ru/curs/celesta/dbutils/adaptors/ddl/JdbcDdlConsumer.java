package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcDdlConsumer implements DdlConsumer {
    @Override
    public void consume(Connection conn, String sql)  {
        if ("COMMIT".equalsIgnoreCase(sql)) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new CelestaException(e);
            }
        } else {
            SqlUtils.executeUpdate(conn, sql);
        }
    }
}
