package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NamedElementTest {

    @Test
    public void testLimitName() {
        final String name = "a_very_long_celesta_identifier_longer_than_max_ident_length";
        String shortcutName = NamedElement.limitName(name);
        assertEquals(NamedElement.MAX_IDENTIFIER_LENGTH, shortcutName.length());
        assertNotEquals(name, shortcutName);
        assertEquals(name.substring(0, NamedElement.MAX_IDENTIFIER_LENGTH - 8),
                shortcutName.substring(0, NamedElement.MAX_IDENTIFIER_LENGTH - 8));
    }

    @Test
    public void testLimitNameWithPostfix() throws ParseException {
        final String name = "my_very_long_table_name";
        String shortcutName = NamedElement.limitName(name, "_nextValueProc");
        //name + postfix length > MAX_IDENTIFIER_LENGTH

        assertEquals(NamedElement.MAX_IDENTIFIER_LENGTH, shortcutName.length());
        assertTrue(shortcutName.endsWith("_nextValueProc"));
        // assert that something is left from the identifier at the beginning
        assertEquals(name.substring(0, 4), shortcutName.substring(0, 4));
    }

    @Test
    public void limitNameFailsForVeryLongPostfix() throws ParseException {
        final String name = "my_very_long_table_name";
        // MAX_IDENTIFIER_LENGTH - postfix length - 8 < 4
        assertThrows(IllegalArgumentException.class,
                () -> NamedElement.limitName(name, "_this_is_too_long_postfix"));
    }
}
