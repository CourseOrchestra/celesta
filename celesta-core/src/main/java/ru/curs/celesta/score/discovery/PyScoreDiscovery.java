package ru.curs.celesta.score.discovery;

import ru.curs.celesta.CelestaException;

import java.io.File;
import java.util.Map;

public class PyScoreDiscovery implements ScoreDiscovery {
    @Override
    public void discoverScore(File scoreDir, Map<String, File> grainFiles) throws CelestaException {
        for (File grainPath : scoreDir.listFiles(File::isDirectory)) {
            String grainName = grainPath.getName();
            File scriptFile = new File(
                    String.format("%s%s_%s.sql", grainPath.getPath(), File.separator, grainName));

            if (scriptFile.exists()) {
                /*
                 * Наличие sql-файла говорит о том, что мы имеем дело с
                 * папкой гранулы, и уже требуем от неё всё подряд.
                 */
                File initFile = new File(String.format("%s%s__init__.py", grainPath.getPath(), File.separator));
                if (!initFile.exists())
                    throw new CelestaException("Cannot find __init__.py in grain '%s' definition folder.",
                            grainName);

                if (!scriptFile.canRead())
                    throw new CelestaException("Cannot read script file '%s'.", scriptFile);
                if (grainFiles.containsKey(grainName))
                    throw new CelestaException("Grain '%s' defined more than once on different paths.", grainName);
                grainFiles.put(grainName, scriptFile);
            }

        }
    }
}
