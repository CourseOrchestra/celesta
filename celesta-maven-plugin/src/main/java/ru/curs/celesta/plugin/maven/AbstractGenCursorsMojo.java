package ru.curs.celesta.plugin.maven;

import ru.curs.celesta.score.*;
import ru.curs.celesta.score.io.FileResource;
import ru.curs.celesta.score.io.Resource;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static ru.curs.celesta.plugin.maven.CursorGenerator.generateCursor;

abstract class AbstractGenCursorsMojo extends AbstractCelestaMojo {

    Supplier<Collection<ScoreProperties>> getScorePaths;
    String generatedSourcesDirName;
    Consumer<String> addCompileSourceRoot;

    @Override
    public void execute() {
        this.getLog().info("celesta project is " + project);
        getScorePaths.get().forEach(this::processScore);
        addSourceRoot(getSourceRoot());
    }

    private void processScore(ScoreProperties properties) {
        final String scorePath = properties.getPath();
        Score score = initScore(scorePath);
        score.getGrains().values()
                .stream()
                .filter(this::isAllowGrain)
                .forEach(g -> generateCursors(g, scorePath));
    }

    private void generateCursors(Grain g, String scorePath) {

        final boolean isSysSchema = g.getName().equals(g.getScore().getSysSchemaName());

        Map<GrainPart, List<GrainElement>> partsToElements = new HashMap<>();

        List<GrainElement> elements = new ArrayList<>();
        elements.addAll(g.getElements(SequenceElement.class).values());
        elements.addAll(g.getElements(Table.class).values());
        elements.addAll(g.getElements(ReadOnlyTable.class).values());
        elements.addAll(g.getElements(View.class).values());
        elements.addAll(g.getElements(MaterializedView.class).values());
        elements.addAll(g.getElements(ParameterizedView.class).values());

        elements.forEach(
                ge -> partsToElements.computeIfAbsent(ge.getGrainPart(), gp -> new ArrayList<>())
                        .add(ge)
        );

        partsToElements.entrySet().stream().forEach(
                e -> {
                    final String sp;
                    if (isSysSchema) {
                        sp = "";
                    } else {
                        final Resource grainPartSource = e.getKey().getSource();
                        final String scoreRelativeOrAbsolutePath = Arrays.stream(scorePath.split(File.pathSeparator))
                                .filter(path -> new FileResource(new File(path)).contains(grainPartSource))
                                .findFirst().get();
                        File scoreDir = new File(scoreRelativeOrAbsolutePath);
                        sp = scoreDir.getAbsolutePath();
                    }
                    e.getValue().forEach(
                            ge -> generateCursor(ge, getSourceRoot(), sp)
                    );
                }
        );

    }

    private File getSourceRoot() {
        return new File(project.getBuild().getDirectory()
                        + File.separator + generatedSourcesDirName + File.separator + "celesta");
    }

    private void addSourceRoot(File directory) {
        if (this.project != null) {
            this.getLog().info("Adding compile source root for cursors: " + directory);
            addCompileSourceRoot.accept(directory.getAbsolutePath());
        }
    }

}
