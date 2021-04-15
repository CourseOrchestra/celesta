package ru.curs.celesta.plugin.maven;

import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

@Mojo(
        name = "gen-cursors",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES
)
public final class GenCursorsMojo extends AbstractGenCursorsMojo {

    @Override
    public void execute() {
        getScorePaths = this::getScorePaths;
        generatedSourcesDirName = "generated-sources";
        addCompileSourceRoot = project::addCompileSourceRoot;

        super.execute();
    }

}
