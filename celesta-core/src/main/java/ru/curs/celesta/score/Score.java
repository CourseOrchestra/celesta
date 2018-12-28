package ru.curs.celesta.score;


import ru.curs.celesta.score.validator.IdentifierParser;
import ru.curs.celesta.score.validator.PlainIdentifierParser;

public final class Score extends AbstractScore {
    public static final String SYSTEM_SCHEMA_NAME = "celesta";

    private IdentifierParser identifierParser = new PlainIdentifierParser();

    public Score() {
    }

    @Override
    public String getSysSchemaName() {
        return SYSTEM_SCHEMA_NAME;
    }

    @Override
    public IdentifierParser getIdentifierParser() {
        return identifierParser;
    }

}
