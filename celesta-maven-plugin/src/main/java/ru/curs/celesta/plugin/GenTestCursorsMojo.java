package ru.curs.celesta.plugin;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.maven.plugins.annotations.*;

@Mojo(
        name = "gen-test-cursors",
        defaultPhase = LifecyclePhase.GENERATE_TEST_SOURCES
)
public final class GenTestCursorsMojo extends AbstractGenCursorsMojo {

    @Override
    String getGeneratedSourceDirName() {
        return "generated-test-sources";
    }

    @Override
    Consumer<String> getAddCompileSourceRootConsumer() {
        return project::addTestCompileSourceRoot;
    }

    @Override
    Collection<ScoreProperties> getScorePaths() {
        List<ScoreProperties> scorePaths = new ArrayList<>();

        File celestaSqlPath = new File(project.getBasedir(), "src/test/celestasql");
        if (celestaSqlPath.exists()) {
            scorePaths.add(new ScoreProperties(celestaSqlPath.getAbsolutePath()));
        }
        scorePaths.addAll(testScores);
        
        return scorePaths;
    }

}
