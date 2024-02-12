package ru.curs.celesta.score;


import ru.curs.celesta.score.validator.IdentifierParser;
import ru.curs.celesta.score.validator.PlainIdentifierParser;

public final class Score extends AbstractScore {
    /**
     * System schema name (which is "celesta").
     */
    public static final String SYSTEM_SCHEMA_NAME = "celesta";

    private final IdentifierParser identifierParser = new PlainIdentifierParser();

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
