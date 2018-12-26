package ru.curs.celesta.score;

/**
 * Foreign key rule type when deleting or updating primary key
 * of the parent record.
 */
public enum FKRule {
    /**
     * Prohibits modification of the parent record.
     */
    NO_ACTION,
    /**
     * Cascade modification of referring records.
     */
    CASCADE,
    /**
     * Setting referring fields to {@code null}.
     */
    SET_NULL
}
