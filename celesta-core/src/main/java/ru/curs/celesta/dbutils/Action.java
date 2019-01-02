package ru.curs.celesta.dbutils;

/**
 * Action type.
 */
public enum Action {

    /**
     * Read.
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
     * Insert.
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
     * Modify.
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
     * Delete.
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
