package ru.curs.celesta.plugin.maven;

import org.apache.maven.plugins.annotations.*;

@Mojo(
        name = "gen-test-cursors",
        defaultPhase = LifecyclePhase.GENERATE_TEST_SOURCES
)
public final class GenTestCursorsMojo extends AbstractGenCursorsMojo {

    @Override
    public void execute() {
        getScorePaths = this::getTestScorePaths;
        generatedSourcesDirName = "generated-test-sources";
        addCompileSourceRoot = project::addTestCompileSourceRoot;

        super.execute();
    }

}
