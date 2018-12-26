package ru.curs.celesta.score.validator;

import ru.curs.celesta.score.ParseException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Identifier parser and validator.
 */
public abstract class IdentifierParser {
    /**
     * Plain name regular expression.
     */
    public static final String PLAIN_NAME_PATTERN_STR = "[a-zA-Z_][0-9a-zA-Z_]*";

    /**
     * Parses the passed in identifier checking it for validity and returns
     * a normalized version of it (e.g. strips down the quotes).
     *
     * @param name  identifier
     * @return
     * @throws ParseException  thrown on validation error.
     */
    public final String parse(String name) throws ParseException {
        validate(name);
        return strip(name);
    }

    /**
     * Validates the passed in identifier.
     *
     * @param name  identifier
     * @return
     * @throws ParseException  thrown on validation error.
     */
    void validate(String name) throws ParseException {
        Matcher m = getNamePattern().matcher(name);
        if (!m.matches()) {
            throw new ParseException(String.format("Invalid identifier: '%s'.", name));
        }
    }

    abstract String strip(String name);


    abstract Pattern getNamePattern();
}
