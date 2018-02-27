package ru.curs.celesta.score.validator;

import java.util.regex.Pattern;

public class PlainIdentifierValidator extends IdentifierValidator {
    private static final Pattern NAME_PATTERN = Pattern.compile(PLAIN_NAME_PATTERN_STR);

    @Override
    Pattern getNamePattern() {
        return NAME_PATTERN;
    }
}
