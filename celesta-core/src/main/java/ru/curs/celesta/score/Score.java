package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.IdentifierValidator;
import ru.curs.celesta.score.validator.PlainIdentifierValidator;

public class Score extends AbstractScore {
    public static final String SYSTEM_SCHEMA_NAME = "celesta";

    private IdentifierValidator identifierValidator = new PlainIdentifierValidator();

    public Score() {}

    @Override
    public String getSysSchemaName() {
        return SYSTEM_SCHEMA_NAME;
    }

    @Override
    public IdentifierValidator getIdentifierValidator() {
        return identifierValidator;
    }
}
