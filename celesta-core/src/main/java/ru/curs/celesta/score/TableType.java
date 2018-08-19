package ru.curs.celesta.score;

import java.util.Arrays;

/**
 * Тип таблицы.
 */
public enum TableType {
    /**
     * Таблица.
     */
    TABLE("T"),
    /**
     * Представление.
     */
    VIEW("V"),
    /**
     * Материализованное представление
     */
    MATERIALIZED_VIEW("MV"),
    /**
     * Параметризованное представление
     */
    FUNCTION("F");

    private String abbreviation;

    TableType(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public static TableType getByAbbreviation(String abbreviation) {
        return Arrays.stream(values())
                .filter(t -> t.abbreviation.equals(abbreviation))
                .findFirst().get();
    }
}
