package ru.curs.celesta.score.discovery;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Namespace;
import ru.curs.celesta.score.io.FileResource;
import ru.curs.celesta.score.io.Resource;

/**
 * Implementation of score discovery based on score path look up.
 */
public final class ScoreByScorePathDiscovery implements ScoreDiscovery {

    private final String scorePath;

    public ScoreByScorePathDiscovery(String scorePath) {
        this.scorePath = scorePath;
    }

    @Override
    public Set<Resource> discoverScore() {

        Set<Resource> scoreSources = new LinkedHashSet<>();

        for (String entry : this.scorePath.split(File.pathSeparator)) {
            File path = new File(entry.trim());
            if (!path.exists()) {
                throw new CelestaException("Score path entry '%s' does not exist.", path.toString());
            }
            if (!path.canRead()) {
                throw new CelestaException("Cannot read score path entry '%s'.", path.toString());
            }
            if (!path.isDirectory()) {
                throw new CelestaException("Score path entry '%s' is not a directory.", path.toString());
            }

            scoreSources.addAll(discoverScore(path));
        }

        return scoreSources;
    }

    private Set<Resource> discoverScore(final File scoreDir) {
        try {
            Path scorePath = scoreDir.toPath().toAbsolutePath();
            return Files.walk(scorePath, FileVisitOption.FOLLOW_LINKS)
                    .filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".sql"))
                    .map(p -> {
                        return new FileResource(p.toFile(), getNamespaceFromPath(scorePath.relativize(p)));
                     })
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (IOException ex) {
            throw new CelestaException(ex);
        }
    }

    Namespace getNamespaceFromPath(Path p) {
        if (p.getNameCount() <= 1) {
            return null;
        }

        StringBuilder sb = new StringBuilder(p.getName(0).toString());
        for (int i = 1; i < p.getNameCount() - 1; i++) {
            sb.append('.').append(p.getName(i).toString());
        }

        return new Namespace(sb.toString().toLowerCase());
    }

}
