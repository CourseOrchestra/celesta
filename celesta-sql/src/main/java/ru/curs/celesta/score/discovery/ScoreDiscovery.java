package ru.curs.celesta.score.discovery;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public interface ScoreDiscovery {

    default Set<File> discoverScore(final File scoreDir) {
        try {
            return Files.walk(scoreDir.toPath())
                    .filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".sql"))
                    .map(Path::toFile)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (IOException e) {
            throw new RuntimeException(e); //TODO: Our analog of RuntimeException must be used
        }
    }
}
