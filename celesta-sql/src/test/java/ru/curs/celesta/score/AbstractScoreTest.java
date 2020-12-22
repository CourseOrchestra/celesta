package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AbstractScoreTest {
    @Test
    void extractLineNo() {
        assertEquals(":5:8 ", AbstractScore.extractLineColNo("at line 5, column 8"));
    }

    @Test
    void emptyStringIfLineNotFound() {
        assertEquals("", AbstractScore.extractLineColNo("at asdf, column 8"));
    }
}