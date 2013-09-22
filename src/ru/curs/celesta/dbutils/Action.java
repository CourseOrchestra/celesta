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
	},
	/**
	 * Вставлять.
	 */
	INSERT {
		@Override
		int getMask() {
			return 2;
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
	},
	/**
	 * Удалять.
	 */
	DELETE {
		@Override
		int getMask() {
			return 8;
		}
	};
	abstract int getMask();
}