package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.AnsiQuotedIdentifierValidator;
import ru.curs.celesta.score.validator.IdentifierValidator;

public class CelestaSqlAnsiQuotedTestScore extends CelestaSqlTestScore {

    private IdentifierValidator identifierValidator = new AnsiQuotedIdentifierValidator();

    @Override
    public IdentifierValidator getIdentifierValidator() {
        return this.identifierValidator;
    }
}
