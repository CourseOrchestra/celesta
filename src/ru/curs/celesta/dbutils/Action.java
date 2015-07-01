package ru.curs.celesta.dbutils;

/**
 * Тип действия.
 * 
 */
public enum Action {

	/**
	 * Читать.
	 */
	READ {
		@Override
		int getMask() {
			return 1;
		}

		@Override
		String shortId() {
			return "R";
		}
	},
	/**
	 * Вставлять.
	 */
	INSERT {
		@Override
		int getMask() {
			return 2;
		}

		@Override
		String shortId() {
			return "I";
		}
	},
	/**
	 * Изменять.
	 */
	MODIFY {
		@Override
		int getMask() {
			return 4;
		}

		@Override
		String shortId() {
			return "M";
		}
	},
	/**
	 * Удалять.
	 */
	DELETE {
		@Override
		int getMask() {
			return 8;
		}

		@Override
		String shortId() {
			return "D";
		}
	};
	abstract int getMask();
	abstract String shortId();
}