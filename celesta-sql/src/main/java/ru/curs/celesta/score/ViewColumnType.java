package ru.curs.celesta.score;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * Тип выражения.
 */
public enum ViewColumnType {
	/**
	 * Логическое условие.
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
		public Class getJavaClass() {
			return Boolean.class;
		}
	},
	/**
	 * Числовое значение с дробной частью.
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
		public Class getJavaClass() {
			return Double.class;
		}
	},
	/**
	 * Числовое значение с дробной частью с фиксированной точкой
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
		public Class getJavaClass() {
			return BigDecimal.class;
		}
	},
	/**
	 * Целочисленное значение.
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
		public Class getJavaClass() {
			return Integer.class;
		}
	},
	/**
	 * Текстовое значение.
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
		public Class getJavaClass() {
			return String.class;
		}
	},
	/**
	 * Дата.
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
		public Class getJavaClass() {
			return Date.class;
		}
	},

	/**
	 * Дата с часовым поясом.
	 */
	DATE_WITH_TIME_ZONE {
		@Override
		public String jdbcGetterName() {
			return "getString";
		}

		@Override
		public String getCelestaType() {
			return ZonedDateTimeColumn.CELESTA_TYPE;
		}

		@Override
		public Class getJavaClass() {
			return ZonedDateTime.class;
		}
	},

	/**
	 * Булевское значение.
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
		public Class getJavaClass() {
			return Boolean.class;
		}
	},
	/**
	 * Большой объект.
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
		public Class getJavaClass() {
			return String.class;
		}
	},
	/**
	 * Неопределённое значение.
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
		public Class getJavaClass() {
			return null;
		}
	};

	/**
	 * JDBC getter.
	 */
	abstract String jdbcGetterName();

	/**
	 * Celesta type.
	 */
	public abstract String getCelestaType();

	public abstract Class getJavaClass();
}