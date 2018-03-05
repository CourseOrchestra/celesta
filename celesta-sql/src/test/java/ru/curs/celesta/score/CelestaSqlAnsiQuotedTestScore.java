package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.AnsiQuotedIdentifierParser;
import ru.curs.celesta.score.validator.IdentifierParser;

public class CelestaSqlAnsiQuotedTestScore extends CelestaSqlTestScore {

    private final IdentifierParser identifierValidator = new AnsiQuotedIdentifierParser();

    @Override
    public IdentifierParser getIdentifierParser() {
        return this.identifierValidator;
    }
}
