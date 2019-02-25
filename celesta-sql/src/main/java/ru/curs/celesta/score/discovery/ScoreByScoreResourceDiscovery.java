package ru.curs.celesta.score.discovery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.io.Resource;
import ru.curs.celesta.score.io.UrlResource;

/**
 * Implementation of score discovery based on JAR-files resources look up.
 */
public final class ScoreByScoreResourceDiscovery implements ScoreDiscovery {

    private static final String SCORE_FILES_LOCATION = "score/score.files";

    @Override
    public Set<Resource> discoverScore() {

        Map<String, Resource> grainNameToResourceMap = new LinkedHashMap<>();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Enumeration<URL> urls = (classLoader != null)
                    ? classLoader.getResources(SCORE_FILES_LOCATION)
                    : ClassLoader.getSystemResources(SCORE_FILES_LOCATION);

            while (urls.hasMoreElements()) {
                Resource scoreFilesResource = new UrlResource(urls.nextElement());

                InputStream scoreFilesInputStream;
                try {
                    scoreFilesInputStream = scoreFilesResource.getInputStream();
                } catch (IOException ex) {
                    System.out.println("score index file is missing: " + scoreFilesResource.toString());
                    continue;
                }

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(scoreFilesInputStream, StandardCharsets.UTF_8))) {
                    Iterable<String> li = reader.lines()::iterator;
                    for (String gp : li) {
                        final String grainName = getGrainName(gp);
                        Resource grainResource = scoreFilesResource.createRelative(gp);
                        Resource existingGrainResource = grainNameToResourceMap.put(grainName, grainResource);
                        if (existingGrainResource != null) {
                            throw new CelestaException("Duplicate resources encountered for the grain '%s': %s, %s",
                                                       grainName, existingGrainResource, grainResource);
                        }
                    }
                }
            }

        } catch (IOException ex) {
            throw new CelestaException("Unable to load score files from resources!", ex);
        }

        return new LinkedHashSet<>(grainNameToResourceMap.values());
    }


    private String getGrainName(String grainPath) {

        String result = grainPath.toLowerCase();

        int i = grainPath.lastIndexOf('/');
        if (i >= 0) {
            result = grainPath.substring(i + 1);
        }

        i = grainPath.lastIndexOf('.');
        if (i >= 0) {
            result = grainPath.substring(0, i);
        }

        return result;
    }

}
