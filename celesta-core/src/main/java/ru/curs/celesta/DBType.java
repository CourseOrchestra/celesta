package ru.curs.celesta;

import java.util.Arrays;

/**
 * DB type.
 */
public enum DBType {
    /**
     * Postgre.
     */
    POSTGRESQL,
    /**
     * MS SQL.
     */
    MSSQL,
    /**
     * ORACLE.
     */
    ORACLE,
    /**
     * FIREBIRD.
     */
    FIREBIRD,
    /**
     * H2.
     */
    H2,
    /**
     * Unknown type.
     */
    UNKNOWN;

    /**
     * Resolves DB type from JDBC URL string.
     *
     * @param url  JDBC URL
     * @return
     */
    public static DBType resolveByJdbcUrl(String url) {
        if (url.startsWith("jdbc:sqlserver")) {
            return DBType.MSSQL;
        } else if (url.startsWith("jdbc:postgresql")) {
            return DBType.POSTGRESQL;
        } else if (url.startsWith("jdbc:oracle")) {
            return DBType.ORACLE;
        } else if (url.startsWith("jdbc:firebirdsql")) {
            return DBType.FIREBIRD;
        } else if (url.startsWith("jdbc:h2")) {
            return DBType.H2;
        } else {
            return DBType.UNKNOWN;
        }
    }

    /**
     * Returns DB type by its name ignoring case.
     *
     * @param name  DB type name
     * @return
     */
    public static DBType getByNameIgnoreCase(String name) {
        return Arrays.stream(values())
                .filter(n -> n.name().equalsIgnoreCase(name))
                .findFirst()
                .get();
    }

}
