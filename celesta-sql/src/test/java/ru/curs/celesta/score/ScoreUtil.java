package ru.curs.celesta.score;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;

public final class ScoreUtil {

    private ScoreUtil() {
        throw new AssertionError();
    }


    public static CelestaSqlTestScore createCelestaSqlTestScore(Class c, String relativePath)
        throws CelestaException, ParseException {
        String scorePath = ResourceUtil.getResourcePath(c, relativePath);

        return new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .path(scorePath)
                .scoreDiscovery(new DefaultScoreDiscovery())
                .build();
    }
}
