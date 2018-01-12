package ru.curs.celesta.score.discovery;

import ru.curs.celesta.CelestaException;

import java.io.File;
import java.util.Map;

public interface ScoreDiscovery {

    default void discoverScore(final File scoreDir, final Map<String, File> grainFiles) throws CelestaException {
        for (File grainPath : scoreDir.listFiles(File::isDirectory)) {
            String grainName = grainPath.getName();
            File scriptFile = new File(
                    String.format("%s%s_%s.sql", grainPath.getPath(), File.separator, grainName));

            if (scriptFile.exists()) {
                /*
                 * Наличие sql-файла говорит о том, что мы имеем дело с
                 * папкой гранулы, и уже требуем от неё всё подряд.
                 */
                if (!scriptFile.canRead())
                    throw new CelestaException("Cannot read script file '%s'.", scriptFile);
                if (grainFiles.containsKey(grainName))
                    throw new CelestaException("Grain '%s' defined more than once on different paths.", grainName);
                grainFiles.put(grainName, scriptFile);
            }

        }
    }
}
