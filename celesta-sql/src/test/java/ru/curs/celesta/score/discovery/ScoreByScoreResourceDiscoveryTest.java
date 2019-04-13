package ru.curs.celesta.score.discovery;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Namespace;
import ru.curs.celesta.score.io.Resource;
import ru.curs.celesta.score.io.UrlResource;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ScoreByScoreResourceDiscoveryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreByScoreResourceDiscoveryTest.class);

    private static final String SCORE_FILES = "score.files";

    private final ScoreByScoreResourceDiscovery scoreDiscovery = new ScoreByScoreResourceDiscovery();

    @Test
    void testGetGrainName() {
        String grainName = scoreDiscovery.getGrainName("data/table/table.sql");
        assertEquals("table", grainName);

        grainName = scoreDiscovery.getGrainName("table.sql");
        assertEquals("table", grainName);

        grainName = scoreDiscovery.getGrainName("data/table/Table.sql");
        assertEquals("table", grainName);

        grainName = scoreDiscovery.getGrainName("Table.sql");
        assertEquals("table", grainName);
    }

    @Test
    void testGetGrainNamespace() {
        Namespace ns = scoreDiscovery.getGrainNamespace("table.sql");
        assertSame(Namespace.DEFAULT, ns);

        ns = scoreDiscovery.getGrainNamespace("data/table/Table.sql");
        assertEquals("data.table", ns.getValue());
    }

    @Test
    void testDiscoverScore_uniqueGrainNames() throws IOException {

        final URL scoreAUrl = getClass().getResource("/scores/resourceDiscoveryScore/score_A/");
        final URL scoreBUrl = getClass().getResource("/scores/resourceDiscoveryScore/score_B/");

        Set<Resource> grainUrls =  scoreDiscovery.discoverScore(Collections.enumeration(Arrays.asList(
                new URL(scoreAUrl, SCORE_FILES),
                new URL(scoreBUrl, SCORE_FILES))));

        Set<Resource> expectedGrainUrls = new HashSet<>(Arrays.asList(
                new UrlResource(scoreAUrl).createRelative("a/score/A.sql"),
                new UrlResource(scoreBUrl).createRelative("b/score/B.sql")));

        assertEquals(expectedGrainUrls, grainUrls);
    }

    @Test
    void testDiscoverScore_missingScoreFiles() throws IOException {

        final URL scoreAUrl = getClass().getResource("/scores/resourceDiscoveryScore/score_A/");
        final URL scoreZUrl = getClass().getResource("/scores/resourceDiscoveryScore/score_Z/");

        Set<Resource> grainUrls =  scoreDiscovery.discoverScore(Collections.enumeration(Arrays.asList(
                new URL(scoreAUrl, SCORE_FILES),
                new URL(scoreZUrl, SCORE_FILES))));

        Set<Resource> expectedGrainUrls = new HashSet<>(Arrays.asList(
                new UrlResource(scoreAUrl).createRelative("a/score/A.sql")));

        assertEquals(expectedGrainUrls, grainUrls);
    }

    @Test
    void testDiscoverScore_nonUniqueGrainNames() throws IOException {

        final URL scoreAUrl = getClass().getResource("/scores/resourceDiscoveryScore/score_A/");
        final URL scoreA1Url = getClass().getResource("/scores/resourceDiscoveryScore/score_A1/");

        CelestaException ex = assertThrows(CelestaException.class, () -> {
            scoreDiscovery.discoverScore(Collections.enumeration(Arrays.asList(
                    new URL(scoreAUrl, SCORE_FILES),
                    new URL(scoreA1Url, SCORE_FILES))));
        });

        LOGGER.info(ex.getMessage());
    }

}
