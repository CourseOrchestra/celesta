package ru.curs.celesta.score.validator;

import java.util.regex.Pattern;

/**
 * ANSI quotes identifier parser and validator.<br/>
 * <br/>
 * Identifiers like <b>"celestaIdentifier"</b> are processed.
 */
public final class AnsiQuotedIdentifierParser extends IdentifierParser {
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "(\"([^\"]+)\")|(" + PLAIN_NAME_PATTERN_STR + ")"
    );

    @Override
    String strip(String name) {
        return name.replace("\"", "");
    }

    @Override
    Pattern getNamePattern() {
        return NAME_PATTERN;
    }
}
