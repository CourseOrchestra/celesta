package ru.curs.celesta.dbutils.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLActionReturning<T> {
    T invoke(ResultSet rs) throws SQLException;
}
