package ru.curs.celesta.score;

import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;

public final class ScoreUtil {

    private ScoreUtil() {
        throw new AssertionError();
    }


    public static CelestaSqlTestScore createCelestaSqlTestScore(Class<?> c, String relativePath) throws ParseException {
        String scorePath = ResourceUtil.getResourcePath(c, relativePath);

        return new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                .build();
    }
}
