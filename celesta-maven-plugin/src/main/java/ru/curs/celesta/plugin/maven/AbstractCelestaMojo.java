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
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;

abstract class AbstractCelestaMojo extends AbstractMojo {

    static final String CELESTASQL_DIR = "src/main/celestasql";
    static final String CELESTASQL_TEST_DIR = "src/test/celestasql";

    @Parameter(property = "scores", required = false)
    List<ScoreProperties> scores = Collections.emptyList();

    @Parameter(property = "testScores", required = false)
    List<ScoreProperties> testScores = Collections.emptyList();

    @Parameter(property = "genSysCursors", required = false)
    boolean genSysCursors;

    @Component
    MavenProject project;

    final Collection<ScoreProperties> getScorePaths() {
        List<ScoreProperties> scorePaths = new ArrayList<>();

        File celestaSqlPath = new File(project.getBasedir(), CELESTASQL_DIR);
        if (celestaSqlPath.exists()) {
            scorePaths.add(new ScoreProperties(celestaSqlPath.getAbsolutePath()));
        }
        scorePaths.addAll(scores);

        return scorePaths;
    }

    final Collection<ScoreProperties> getTestScorePaths() {
        List<ScoreProperties> scorePaths = new ArrayList<>();

        File celestaSqlPath = new File(project.getBasedir(), CELESTASQL_TEST_DIR);
        if (celestaSqlPath.exists()) {
            scorePaths.add(new ScoreProperties(celestaSqlPath.getAbsolutePath()));
        }
        scorePaths.addAll(testScores);

        return scorePaths;
    }

    final Score initScore(String scorePath) {
        try {
            Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                    .path(scorePath)
                    .scoreDiscovery(new DefaultScoreDiscovery())
                    .build();
            return score;
        } catch (CelestaException | ParseException e) {
            throw new CelestaException("Can't init score", e);
        }
    }

    final boolean isAllowGrain(Grain grain) {
        return genSysCursors || !Objects.equals(grain.getScore().getSysSchemaName(), grain.getName());
    }

}
