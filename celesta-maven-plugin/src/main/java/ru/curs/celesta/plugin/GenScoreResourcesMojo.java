package ru.curs.celesta.plugin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.*;

import ru.curs.celesta.score.Score;

@Mojo(
        name = "gen-score-resources",
        defaultPhase = LifecyclePhase.GENERATE_RESOURCES
)
public final class GenScoreResourcesMojo extends AbstractCelestaMojo {

    static final String SCORE_FILES_FILE_NAME = "score.files";

    Supplier<Collection<ScoreProperties>> getScorePaths;
    String generatedResourcesDirName;

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException {
        getScorePaths = this::getScorePaths;
        generatedResourcesDirName = "generated-resources";
        
        this.getLog().info("celesta project is " + project);
        
        List<GrainSourceBag> grainsSources = new ArrayList<>();
        for (ScoreProperties sp : getScorePaths.get()) {
            Score score = initScore(sp.getPath());
            File scorePath = new File(score.getPath());
            score.getGrains().values().stream()
                .filter(this::isAllowGrain)
                .flatMap((g) -> g.getGrainParts().stream())
                .map((gp) -> gp.getSourceFile())
                .filter(Objects::nonNull)
                .forEach((sf) -> {
                    grainsSources.add(new GrainSourceBag(scorePath.toPath(), sf.toPath()));
                });
        }
        
        copyGrainSourceFilesToResources(grainsSources);
        generateScoreFiles(grainsSources);
    }

    private void copyGrainSourceFilesToResources(
            Collection<GrainSourceBag> grainSources) throws MojoExecutionException {
        
        Path resourcesRootPath = getResourcesRoot().toPath();
        for (GrainSourceBag gs : grainSources) {
            Path to = gs.resolve(resourcesRootPath);
            try {
                Files.createDirectories(to.getParent());
                Files.copy(gs.grainSourcePath, to);
            } catch (IOException ex) {
                throw new MojoExecutionException(
                        String.format("Copying of grain source file failed: %s", gs.grainSourcePath),
                        ex);
            }
        }
    }

    private File getResourcesRoot() {
        return new File(project.getBuild().getDirectory()
                        + File.separator + generatedResourcesDirName + File.separator + "score");
    }

    private void generateScoreFiles(List<GrainSourceBag> grainsSources) throws MojoExecutionException {
        
        Collection<String> relativeSourcesPaths = grainsSources.stream()
                .map((gs) -> gs.getGrainSourceRelativePath().toString())
                .collect(Collectors.toCollection(TreeSet::new));
        
        Path scoreFilesPath = new File(getResourcesRoot(), SCORE_FILES_FILE_NAME).toPath();
        try {
            Files.write(scoreFilesPath, relativeSourcesPaths);
        } catch (IOException ex) {
            throw new MojoExecutionException("Error writing a score.files", ex);
        }
    }

    private static class GrainSourceBag {
        final Path scorePath;
        final Path grainSourcePath;
        GrainSourceBag(Path scorePath, Path grainSourcePath) {
            this.scorePath = scorePath.toAbsolutePath();
            this.grainSourcePath = grainSourcePath.toAbsolutePath();
        }
        Path resolve(Path rootPath) {
            return rootPath.resolve(getGrainSourceRelativePath());
        }
        Path getGrainSourceRelativePath() {
            return scorePath.relativize(grainSourcePath);
        }
    }

}
