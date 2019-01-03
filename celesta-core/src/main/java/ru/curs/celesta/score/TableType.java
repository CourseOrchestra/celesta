package ru.curs.celesta.score;

import java.util.Arrays;

/**
 * Table type.
 */
public enum TableType {
    /**
     * Table.
     */
    TABLE("T"),
    /**
     * View.
     */
    VIEW("V"),
    /**
     * Materialized view.
     */
    MATERIALIZED_VIEW("MV"),
    /**
     * Parameterized view.
     */
    FUNCTION("F");

    private final String abbreviation;

    TableType(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    /**
     * Returns abbreviation for the table type.
     * @return
     */
    public String getAbbreviation() {
        return abbreviation;
    }

    public static TableType getByAbbreviation(String abbreviation) {
        return Arrays.stream(values())
                .filter(t -> t.abbreviation.equals(abbreviation))
                .findFirst().get();
    }
}
