package ru.curs.celesta.plugin.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.*;

@Mojo(
        name = "gen-score-resources",
        defaultPhase = LifecyclePhase.GENERATE_RESOURCES
)
public final class GenScoreResourcesMojo extends AbstractGenScoreResourcesMojo {

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getScorePaths = this::getScorePaths;
        generatedResourcesDirName = "generated-resources";
        addResource = project::addResource;

        super.execute();
    }

}
