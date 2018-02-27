package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.IdentifierValidator;
import ru.curs.celesta.score.validator.PlainIdentifierValidator;

public class CelestaSqlTestScore extends AbstractScore {

    private IdentifierValidator identifierValidator = new PlainIdentifierValidator();

    public CelestaSqlTestScore() {
    }

    @Override
    public String getSysSchemaName() {
        return "celestaSql";
    }

    @Override
    public IdentifierValidator getIdentifierValidator() {
        return identifierValidator;
    }

}
