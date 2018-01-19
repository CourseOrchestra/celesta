package ru.curs.celesta.score;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.discovery.ScoreDiscovery;

import java.io.InputStream;

public class CelestaSqlTestScore extends AbstractScore {

    public CelestaSqlTestScore() {
    }

    @SuppressWarnings("unused")
    public CelestaSqlTestScore(String scorePath, ScoreDiscovery scoreDiscovery) throws CelestaException {
        super(scorePath, scoreDiscovery);
    }

    @Override
    public String getSysSchemaName() {
        return "celestaSql";
    }
}
