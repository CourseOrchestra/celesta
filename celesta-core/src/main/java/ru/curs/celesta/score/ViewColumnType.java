package ru.curs.celesta.score;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * Expression type.
 */
public enum ViewColumnType {
    /**
     * Logical condition.
     */
    LOGIC {
        @Override
        public String jdbcGetterName() {
            return null;
        }

        @Override
        public String getCelestaType() {
            return BooleanColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return Boolean.class;
        }
    },
    /**
     * Numeric value with fractional part.
     */
    REAL {
        @Override
        public String jdbcGetterName() {
            return "getDouble";
        }

        @Override
        public String getCelestaType() {
            return FloatingColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return Double.class;
        }
    },
    /**
     * Numeric value with fractional part and fixed decimal point.
     */
    DECIMAL {
        @Override
        public String jdbcGetterName() {
            return "getBigDecimal";
        }

        @Override
        public String getCelestaType() {
            return DecimalColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return BigDecimal.class;
        }
    },
    /**
     * Integer value.
     */
    INT {
        @Override
        public String jdbcGetterName() {
            return "getInt";
        }

        @Override
        public String getCelestaType() {
            return IntegerColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return Integer.class;
        }
    },
    /**
     * Text value.
     */
    TEXT {
        @Override
        public String jdbcGetterName() {
            return "getString";
        }

        @Override
        public String getCelestaType() {
            return StringColumn.VARCHAR;
        }

        @Override
        public Class<?> getJavaClass() {
            return String.class;
        }
    },
    /**
     * Date.
     */
    DATE {
        @Override
        public String jdbcGetterName() {
            return "getTimestamp";
        }

        @Override
        public String getCelestaType() {
            return DateTimeColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return Date.class;
        }
    },

    /**
     * Date with time zone.
     */
    DATE_WITH_TIME_ZONE {
        @Override
        public String jdbcGetterName() {
            return "getTimestamp";
        }

        @Override
        public String getCelestaType() {
            return ZonedDateTimeColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return ZonedDateTime.class;
        }
    },

    /**
     * Boolean value.
     */
    BIT {
        @Override
        public String jdbcGetterName() {
            return "getBoolean";
        }

        @Override
        public String getCelestaType() {
            return BooleanColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return Boolean.class;
        }
    },
    /**
     * Binary large object.
     */
    BLOB {
        @Override
        public String jdbcGetterName() {
            return "getBlob";
        }

        @Override
        public String getCelestaType() {
            return BinaryColumn.CELESTA_TYPE;
        }

        @Override
        public Class<?> getJavaClass() {
            return String.class;
        }
    },
    /**
     * Undefined value.
     */
    UNDEFINED {
        @Override
        public String jdbcGetterName() {
            return null;
        }

        @Override
        public String getCelestaType() {
            return null;
        }

        @Override
        public Class<?> getJavaClass() {
            return null;
        }
    };

    /**
     * JDBC getter.
     *
     * @return
     */
    abstract String jdbcGetterName();

    /**
     * Celesta type.
     *
     * @return
     */
    public abstract String getCelestaType();

    /**
     * Returns Java type.
     *
     * @return
     */
    public abstract Class<?> getJavaClass();

}
