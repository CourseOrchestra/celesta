package ru.curs.celesta.score.discovery;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.io.FileResource;
import ru.curs.celesta.score.io.Resource;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CombinedScoreDiscoveryTest {
    @Test
    void getCombinedScore() {
        Resource resource1 = new FileResource(new File("a"));
        Resource resource2 = new FileResource(new File("b"));

        ScoreDiscovery discovery1 = new ScoreDiscovery() {
            @Override
            public Set<Resource> discoverScore() {
                return Collections.singleton(resource1);
            }
        };
        ScoreDiscovery discovery2 = new ScoreDiscovery() {
            @Override
            public Set<Resource> discoverScore() {
                return Collections.singleton(resource2);
            }
        };
        ScoreDiscovery combinedDiscovery = new CombinedScoreDiscovery(discovery1, discovery2);
        HashSet<Resource> expected = new HashSet<>(Arrays.asList(resource1, resource2));
        assertEquals(expected,
                combinedDiscovery.discoverScore());
    }

    @Test
    void getCombinedEmptyScore() {
        assertEquals(Collections.emptySet(), new CombinedScoreDiscovery().discoverScore());
    }
}