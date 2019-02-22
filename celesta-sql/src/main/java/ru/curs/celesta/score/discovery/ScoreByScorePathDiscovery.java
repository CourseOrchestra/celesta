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

public final class ScoreByScorePathDiscovery implements ScoreDiscovery {

    private final String scorePath;

    public ScoreByScorePathDiscovery(String scorePath) {
        this.scorePath = scorePath;
    }

    @Override
    public Set<File> discoverScore() {

        Set<File> scoreFiles = new LinkedHashSet<>();

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

            scoreFiles.addAll(discoverScore(path));
        }

        return scoreFiles;
    }

    private Set<File> discoverScore(final File scoreDir) {
        try {
            return Files.walk(scoreDir.toPath(), FileVisitOption.FOLLOW_LINKS)
                    .filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".sql"))
                    .map(Path::toFile)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (IOException ex) {
            throw new CelestaException(ex);
        }
    }

}
