package ru.curs.celesta.score.validator;

import ru.curs.celesta.score.ParseException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class IdentifierValidator {
    static final String PLAIN_NAME_PATTERN_STR = "[a-zA-Z_][0-9a-zA-Z_]*";

    public void validate(String name) throws ParseException {
        Matcher m = getNamePattern().matcher(name);
        if (!m.matches())
            throw new ParseException(String.format("Invalid identifier: '%s'.", name));
    }

    abstract Pattern getNamePattern();
}
