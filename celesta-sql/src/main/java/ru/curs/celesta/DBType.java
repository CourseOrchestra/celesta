package ru.curs.celesta;

import java.util.Arrays;

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
     * H2
     */
    H2 {
        @Override
        public String getDriverClassName() {
            return "org.h2.Driver";
        }
    },
    /**
     * Неизвестный тип.
     */
    UNKNOWN {
        @Override
        public String getDriverClassName() {
            return "";
        }
    };

    abstract public String getDriverClassName();

    public static DBType getByNameIgnoreCase(String name) {
        return Arrays.stream(values())
                .filter(n -> n.name().equalsIgnoreCase(name))
                .findFirst()
                .get();
    }
}
