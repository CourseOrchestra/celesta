package ru.curs.celesta.plugin.maven;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Namespace;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.io.FileResource;
import ru.curs.celesta.score.io.Resource;

abstract class AbstractGenScoreResourcesMojo extends AbstractCelestaMojo {

    static final String SCORE_FILES_FILE_NAME = "score.files";

    Supplier<Collection<ScoreProperties>> getScorePaths;
    String generatedResourcesDirName;
    Consumer<org.apache.maven.model.Resource> addResource;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        this.getLog().info("Celesta project is " + project);

        List<GrainSourceBag> grainsSources = new ArrayList<>();
        for (ScoreProperties sp : getScorePaths.get()) {
            final String scorePath = sp.getPath();
            List<FileResource> scoreResources =  Arrays.stream(scorePath.split(File.pathSeparator))
                    .map(File::new)
                    .map(File::getAbsoluteFile)
                    .map(FileResource::new)
                    .collect(Collectors.toList());
            Score score = initScore(scorePath);
            score.getGrains().values().stream()
                .filter(this::isAllowGrain)
                .flatMap(g -> g.getGrainParts().stream())
                .forEach(gp -> {
                    if (gp.getSource() != null) {
                        if (Namespace.DEFAULT.equals(gp.getNamespace())) {
                            throw new CelestaException(
                                "Couldn't generate score resource for %s without package",
                                gp.getSource());
                        }
                        FileResource scoreSource = scoreResources.stream()
                            .filter(sr -> sr.contains(gp.getSource()))
                            .findFirst().get();
                        grainsSources.add(new GrainSourceBag(scoreSource, gp.getSource()));
                    }
                });
        }

        if (grainsSources.isEmpty()) {
            return;
        }

        copyGrainSourceFilesToResources(grainsSources);
        generateScoreFiles(grainsSources);

        org.apache.maven.model.Resource scoreResource = new org.apache.maven.model.Resource();
        scoreResource.setDirectory(getResourcesRoot().getAbsolutePath());
        scoreResource.setTargetPath("score");

        addResource.accept(scoreResource);
    }

    private void copyGrainSourceFilesToResources(
            Collection<GrainSourceBag> grainSources) throws MojoExecutionException {

        final Path resourcesRootPath = getResourcesRoot().toPath();

        Set<Path> tos = new HashSet<>();

        for (GrainSourceBag gs : grainSources) {
            Path to = gs.resolve(resourcesRootPath);
            if (to == null) {
                continue;
            }

            if (!tos.add(to)) {
                throw new MojoExecutionException(
                        String.format("There are more than one grain source files being copied to %s", to));
            }

            try {
                Path toParent = to.getParent();
                if (toParent != null) {
                    Files.createDirectories(toParent);
                }
                Files.copy(gs.grainSource.getInputStream(), to, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                throw new MojoExecutionException(
                        String.format("Copying of grain source file failed: %s", gs.grainSource),
                        ex);
            }
        }
    }

    private File getResourcesRoot() {
        return new File(project.getBuild().getDirectory()
                        + File.separator + generatedResourcesDirName + File.separator + "score");
    }

    private String convertSeparatorChar(String path) {
        if (File.separatorChar != '/') {
            return path.replace(File.separatorChar, '/');
        } else {
            return path;
        }
    }

    private void generateScoreFiles(List<GrainSourceBag> grainsSources) throws MojoExecutionException {

        Collection<String> relativeSourcesPaths = grainsSources.stream()
                .map(gs -> gs.getGrainSourceRelativePath().toString())
                .map(this::convertSeparatorChar)
                .collect(Collectors.toCollection(TreeSet::new));

        Path scoreFilesPath = new File(getResourcesRoot(), SCORE_FILES_FILE_NAME).toPath();
        try {
            Files.write(scoreFilesPath, relativeSourcesPaths);
        } catch (IOException ex) {
            throw new MojoExecutionException("Error writing a score.files", ex);
        }
    }

    private static class GrainSourceBag {
        final Resource scoreSource;
        final Resource grainSource;
        GrainSourceBag(Resource scoreSource, Resource grainSource) {
            this.scoreSource = scoreSource;
            this.grainSource = grainSource;
        }
        Path resolve(Path rootPath) {
            return rootPath.resolve(getGrainSourceRelativePath());
        }
        Path getGrainSourceRelativePath() {
            return new File(scoreSource.getRelativePath(grainSource)).toPath();
        }
    }

}
