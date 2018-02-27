package ru.curs.celesta.score.validator;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.ParseException;

import static org.junit.jupiter.api.Assertions.*;

public class IdentifierValidatorTest {

    private PlainIdentifierValidator plainIdentifierValidator = new PlainIdentifierValidator();
    private AnsiQuotedIdentifierValidator ansiQuotedIdentifierValidator = new AnsiQuotedIdentifierValidator();

    @Test
    void testPlainIdentifier() throws Exception {
        String name = "identifier";
        plainIdentifierValidator.validate(name);
        ansiQuotedIdentifierValidator.validate(name);
    }

    @Test
    void testAnsiQuotedIdentifier() throws Exception {
        String name = "\"Russian word: Опаньки!!\"";
        assertThrows(ParseException.class, () -> plainIdentifierValidator.validate(name));
        ansiQuotedIdentifierValidator.validate(name);
    }

    @Test
    void testEmptyIdentifier() {
        String name = "";
        assertAll(
                () -> assertThrows(ParseException.class, () -> plainIdentifierValidator.validate(name)),
                () -> assertThrows(ParseException.class, () -> ansiQuotedIdentifierValidator.validate(name))
        );
    }

    @Test
    void testNotClosedQuotedIdentifiers() {
        String name1 = "\"Russian word: Опаньки!!";
        String name2 = "Russian word: Опаньки!!\"";
        assertAll(
                () -> assertThrows(ParseException.class, () -> plainIdentifierValidator.validate(name1)),
                () -> assertThrows(ParseException.class, () -> ansiQuotedIdentifierValidator.validate(name1)),
                () -> assertThrows(ParseException.class, () -> plainIdentifierValidator.validate(name2)),
                () -> assertThrows(ParseException.class, () -> ansiQuotedIdentifierValidator.validate(name2))
        );
    }
}
