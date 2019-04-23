package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;

import java.io.File;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.*;
import static ru.curs.celesta.score.AbstractScore.CYCLIC_REFERENCES_ERROR_TEMPLATE;
import static ru.curs.celesta.score.AbstractScore.DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE;


public class ParseScoreWithReferencesTest {
    private static final String SCORE_PATH_PREFIX = new StringJoiner(File.separator)
            .add("src").add("test").add("resources").add("scores").add("scoresWithReferences").toString();

    private static final String SCORE_WITH_REFERENCE_TO_NOT_EXISTING_SCHEMA = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("referenceToNotExistingSchema").toString();
    private static final String SCORE_WITH_SELF_REFERENCE = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("selfReference").toString();
    private static final String SCORE_WITH_REFERENCE_TO_EXISTING_SCHEMA = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("referenceToExistingSchema").toString();
    private static final String SCORE_WITH_CYCLIC_REFERENCES = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("cyclicReferences").toString();


    @Test
    void testReferenceToNotExistingSchema() {
        AbstractScore.ScoreBuilder<?> scoreBuilder = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_WITH_REFERENCE_TO_NOT_EXISTING_SCHEMA));

        CelestaException e = assertThrows(CelestaException.class, () -> scoreBuilder.build());
        String expectedMessage = String.format(DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE, "a", "b");
        assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    void testSelfReference() throws Exception {
        new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_WITH_SELF_REFERENCE))
                .build();
    }

    @Test
    void testReferenceToExistingSchema() throws Exception {
        new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_WITH_REFERENCE_TO_EXISTING_SCHEMA))
                .build();
    }

    @Test
    void testCyclicReferences() {
        AbstractScore.ScoreBuilder<?> scoreBuilder = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_WITH_CYCLIC_REFERENCES));

        ParseException e = assertThrows(ParseException.class, scoreBuilder::build);

        String expectedMessagePart1 = String.format(
                CYCLIC_REFERENCES_ERROR_TEMPLATE,
                "a", "a", "b"
        );
        String expectedMessagePart2 = String.format(
                CYCLIC_REFERENCES_ERROR_TEMPLATE,
                "b", "b", "a"
        );
        assertTrue(
                e.getMessage().contains(expectedMessagePart1) || e.getMessage().contains(expectedMessagePart2)
        );
    }
}
