package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class NamedElementTest {

    @Test
    public void testLimitNameWithPostfix() throws ParseException {
        final String name = "my_very_long_table_name";
        String shortcutName = NamedElement.limitName(name, "_nextValueProc");
        assertEquals(NamedElement.MAX_IDENTIFIER_LENGTH, shortcutName.length());
        assertTrue(shortcutName.endsWith("_nextValueProc"));
        // assert that something is left from the identifier at the beginning
        assertEquals(name.substring(0, 4), shortcutName.substring(0, 4));
    }

}
