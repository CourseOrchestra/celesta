package ru.curs.celesta.score;

public class Score extends AbstractScore {
    public static final String SYSTEM_SCHEMA_NAME = "celesta";


    public Score() {}

    @Override
    public String getSysSchemaName() {
        return SYSTEM_SCHEMA_NAME;
    }
}
