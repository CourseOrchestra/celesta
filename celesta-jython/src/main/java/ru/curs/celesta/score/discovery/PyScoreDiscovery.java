package ru.curs.celesta.score.discovery;

import java.io.File;
import java.util.Set;

public class PyScoreDiscovery implements ScoreDiscovery {
    @Override
    public Set<File> discoverScore(File scoreDir) {
        ScoreDiscovery scoreDiscovery = new DefaultScoreDiscovery();
        Set<File> result = scoreDiscovery.discoverScore(scoreDir);

        result.stream()
                .filter(f -> {
                    File initFile = new File(String.format("%s%s__init__.py", f.getParentFile().getPath(), File.separator));
                    return !initFile.exists();
                })
                .findFirst().ifPresent(f -> {
            throw new RuntimeException( //TODO: Our analog of RuntimeException must be used
                    String.format("Cannot find __init__.py in '%s' folder.",
                            f.getParentFile().getPath()));
        });

        return result;
    }

}
