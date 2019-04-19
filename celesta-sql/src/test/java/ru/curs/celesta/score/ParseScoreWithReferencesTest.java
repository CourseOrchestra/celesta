package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;

import java.io.File;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.curs.celesta.score.AbstractScore.CYCLIC_REFERENCES_ERROR_TEMPLATE;
import static ru.curs.celesta.score.AbstractScore.DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE;
import static ru.curs.celesta.score.AbstractScore.GRAIN_PART_PARSING_ERROR_TEMPLATE;

public class ParseScoreWithReferencesTest {

    private static final String SCORE_PATH_PREFIX = new StringJoiner(File.separator)
            .add("src").add("test").add("resources").add("scores").add("scoresWithReferences").toString();

    private static final String SCORE_WITH_REFERENCE_TO_NOT_EXISTING_SCHEMA = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("referenceToNotExistingSchema").toString();

    private static final String SCORE_WITH_CYCLIC_REFERENCES = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("cyclicReferences").toString();

    @Test
    void testReferenceToNotExistingSchema() {
        AbstractScore.ScoreBuilder<?> scoreBuilder = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new DefaultScoreDiscovery())
                .path(SCORE_WITH_REFERENCE_TO_NOT_EXISTING_SCHEMA);

        ParseException e = assertThrows(ParseException.class, scoreBuilder::build);

        String expectedDescription = String.format(DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE, "a", "b");
        String expectedMessage = String.format(
                GRAIN_PART_PARSING_ERROR_TEMPLATE,
                SCORE_WITH_REFERENCE_TO_NOT_EXISTING_SCHEMA + File.separator + "schema.sql",
                expectedDescription
        );
        assertEquals(expectedMessage, e.getMessage());
    }


    @Test
    void testCyclicReferences() {
        AbstractScore.ScoreBuilder<?> scoreBuilder = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new DefaultScoreDiscovery())
                .path(SCORE_WITH_CYCLIC_REFERENCES);

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
