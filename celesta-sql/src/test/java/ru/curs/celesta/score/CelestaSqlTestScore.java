package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.IdentifierParser;
import ru.curs.celesta.score.validator.PlainIdentifierParser;

public class CelestaSqlTestScore extends AbstractScore {

    private final IdentifierParser identifierValidator = new PlainIdentifierParser();

    public CelestaSqlTestScore() {
    }

    @Override
    public String getSysSchemaName() {
        return "celestaSql";
    }

    @Override
    public IdentifierParser getIdentifierParser() {
        return identifierValidator;
    }

}
