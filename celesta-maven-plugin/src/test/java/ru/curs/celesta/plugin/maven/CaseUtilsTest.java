package ru.curs.celesta.plugin.maven;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CaseUtilsTest {
    @Test
    void snakeToCamel() {
        assertEquals("thisIsSnake", CaseUtils.snakeToCamel("this_is_snake"));
    }

    @Test
    void snakeToCamelNull() {
        assertNull(CaseUtils.snakeToCamel(null));
    }

    @Test
    void snakeToCamelEmpty() {
        assertEquals("", CaseUtils.snakeToCamel(""));
    }

    @Test
    void snakeToCamelUnderscore() {
        assertEquals("_", CaseUtils.snakeToCamel("_"));
    }

    @Test
    void snakeToCamelMultiUnderscore() {
        assertEquals("__", CaseUtils.snakeToCamel("__"));
    }

    @Test
    void snakeToCamelMultiUnderscoreInText() {
        assertEquals("__thisIsSnake", CaseUtils.snakeToCamel("__this__is__snake_"));
    }

    @Test
    void snakeToCamelAlreadyCamel() {
        assertEquals("thisIsCamel", CaseUtils.snakeToCamel("thisIsCamel"));
    }

    @Test
    void capitalize() {
        assertEquals("Foo", CaseUtils.capitalize("foo"));
    }

    @Test
    void capitalizeNull() {
        assertNull(CaseUtils.capitalize(null));
    }

    @Test
    void capitalizeEmpty() {
        assertEquals("", CaseUtils.capitalize(""));
    }

    @Test
    void capitalizeSingleChar() {
        assertEquals("A", CaseUtils.capitalize("a"));
    }

    @Test
    void capitalizeUnderscore() {
        assertEquals("_foo", CaseUtils.capitalize("_foo"));
    }
}
