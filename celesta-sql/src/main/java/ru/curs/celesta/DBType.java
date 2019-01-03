package ru.curs.celesta;

import java.util.Arrays;

/**
 * DB type.
 */
public enum DBType {
    /**
     * Postgre.
     */
    POSTGRESQL {
        @Override
        public String getDriverClassName() {
            return "org.postgresql.Driver";
        }
    },
    /**
     * MS SQL.
     */
    MSSQL {
        @Override
        public String getDriverClassName() {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
    },
    /**
     * ORACLE.
     */
    ORACLE {
        @Override
        public String getDriverClassName() {
            return "oracle.jdbc.driver.OracleDriver";
        }
    },
    /**
     * H2.
     */
    H2 {
        @Override
        public String getDriverClassName() {
            return "org.h2.Driver";
        }
    },
    /**
     * Unknown type.
     */
    UNKNOWN {
        @Override
        public String getDriverClassName() {
            return "";
        }
    };

    /**
     * Returns JDBC driver class name.
     *
     * @return
     */
    public abstract String getDriverClassName();

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
