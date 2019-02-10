package ru.curs.celesta.plugin;

import org.apache.maven.plugins.annotations.*;

@Mojo(
        name = "gen-cursors",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES
)
public final class GenCursorsMojo extends AbstractGenCursorsMojo {

    @Override
    public final void execute() {
        getScorePaths = this::getScorePaths;
        generatedSourcesDirName = "generated-sources";
        addCompileSourceRoot = project::addCompileSourceRoot;

        super.execute();
    }

}
