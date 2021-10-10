package ru.curs.celesta.plugin.maven;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;

abstract class AbstractCelestaMojo extends AbstractMojo {

    static final String CELESTASQL_DIR = "src/main/celestasql";
    static final String CELESTASQL_TEST_DIR = "src/test/celestasql";

    @Parameter(property = "scores")
    List<ScoreProperties> scores = Collections.emptyList();

    @Parameter(property = "testScores")
    List<ScoreProperties> testScores = Collections.emptyList();

    @Parameter(property = "snakeToCamel")
    boolean snakeToCamel = true;

    @Parameter(property = "genSysCursors")
    boolean genSysCursors;

    @Component
    MavenProject project;

    final Collection<ScoreProperties> getScorePaths() {
        return getPaths(CELESTASQL_DIR, scores);
    }

    final Collection<ScoreProperties> getTestScorePaths() {
        return getPaths(CELESTASQL_TEST_DIR, testScores);
    }

    private Collection<ScoreProperties> getPaths(String celestasqlDir, List<ScoreProperties> scoresCollection) {
        List<ScoreProperties> scorePaths = new ArrayList<>();

        File celestaSqlPath = new File(project.getBasedir(), celestasqlDir);
        if (celestaSqlPath.exists()) {
            scorePaths.add(new ScoreProperties(celestaSqlPath.getAbsolutePath()));
        }
        scorePaths.addAll(scoresCollection);

        return scorePaths;
    }

    final Score initScore(String scorePath) {
        try {
            Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                    .build();
            return score;
        } catch (ParseException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

    final boolean isAllowGrain(Grain grain) {
        return genSysCursors || !Objects.equals(grain.getScore().getSysSchemaName(), grain.getName());
    }

    boolean isSnakeToCamel() {
        return snakeToCamel;
    }
}
