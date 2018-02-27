package ru.curs.celesta.score.validator;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.NamedElement;
import ru.curs.celesta.score.ParseException;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    @Test
    void testNameLengthLimit() throws Exception {
        String shortName = IntStream.range(0, NamedElement.MAX_IDENTIFIER_LENGTH)
                .boxed().map(i -> "a").collect(Collectors.joining());
        String longName = IntStream.range(0, NamedElement.MAX_IDENTIFIER_LENGTH + 1)
                .boxed().map(i -> "a").collect(Collectors.joining());

        plainIdentifierValidator.validate(shortName);
        ansiQuotedIdentifierValidator.validate(shortName);
        assertThrows(ParseException.class, () ->plainIdentifierValidator.validate(longName));
        ansiQuotedIdentifierValidator.validate(longName);
    }
}
