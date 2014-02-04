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
	},
	/**
	 * Числовое значение.
	 */
	NUMERIC {
		@Override
		public String jdbcGetterName() {
			return "getDouble";
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
	},
	/**
	 * Дата.
	 */
	DATE {
		@Override
		public String jdbcGetterName() {
			return "getTimestamp";
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
	},
	/**
	 * Большой объект.
	 */
	BLOB {
		@Override
		public String jdbcGetterName() {
			return "getBlob";
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
	};
	/**
	 * Имя JDBC-геттера, подходящего для данного типа колонки. Необходимо для
	 * процедур генерации ORM-кода.
	 */
	public abstract String jdbcGetterName();
}