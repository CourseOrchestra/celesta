package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlRunner {
    
    public void executeUpdate(Connection conn, String sql) throws CelestaException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }
    
}
