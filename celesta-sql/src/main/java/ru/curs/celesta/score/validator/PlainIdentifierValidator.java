package ru.curs.celesta.score.validator;

import ru.curs.celesta.score.NamedElement;
import ru.curs.celesta.score.ParseException;

import java.util.regex.Pattern;

public class PlainIdentifierValidator extends IdentifierValidator {
    private static final Pattern NAME_PATTERN = Pattern.compile(PLAIN_NAME_PATTERN_STR);

    @Override
    public void validate(String name) throws ParseException {
        super.validate(name);
        if (name.length() > NamedElement.MAX_IDENTIFIER_LENGTH)
            throw new ParseException(
                    String.format("Identifier '%s' is longer than %d characters.", name, NamedElement.MAX_IDENTIFIER_LENGTH));
    }

    @Override
    Pattern getNamePattern() {
        return NAME_PATTERN;
    }
}
