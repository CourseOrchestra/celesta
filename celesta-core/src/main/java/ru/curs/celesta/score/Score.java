package ru.curs.celesta.score;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.discovery.ScoreDiscovery;

public class Score extends AbstractScore {
    public static final String SYSTEM_SCHEMA_NAME = "celesta";


    public Score() {}

    public Score(String scorePath, ScoreDiscovery scoreDiscovery) throws CelestaException {
        super(scorePath, scoreDiscovery);
    }

    @Override
    public String getSysSchemaName() {
        return SYSTEM_SCHEMA_NAME;
    }
}
