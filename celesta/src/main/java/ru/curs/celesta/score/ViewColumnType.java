package ru.curs.celesta.score;

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

	};

	/**
	 * JDBC getter.
	 */
	abstract String jdbcGetterName();

	/**
	 * Celesta type.
	 */
	public abstract String getCelestaType();
}