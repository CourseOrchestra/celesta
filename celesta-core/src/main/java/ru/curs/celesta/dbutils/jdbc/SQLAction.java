package ru.curs.celesta.dbutils.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLAction {
    void invoke(ResultSet rs) throws SQLException;
}
