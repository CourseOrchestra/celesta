package ru.curs.celesta.score.discovery;


import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.ScoreUtil;
import ru.curs.celesta.score.SequenceElement;

import static org.junit.jupiter.api.Assertions.*;

public class ScoreDiscoveryTest {

    @Test
    void testMultipleFilesSchemaWithFlatLayout() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "multi_files_schema"
        );
        testMultipleFilesSchema(s);
    }

    @Test
    void testMultipleFilesSchemaWithReversedFlatLayout() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "multi_files_schema_reverse"
        );
        testMultipleFilesSchema(s);
    }

    @Test
    void testParseExceptionOccursWithoutSchemaDefinition() {
        assertThrows(
                ParseException.class,
                () -> ScoreUtil.createCelestaSqlTestScore(
                        this.getClass(),
                        "no_schema_definition"
                )
        );
    }

    @Test
    void testTwoSchemasWithFlatLayout() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "two_schemas"
        );

        Grain g1 = s.getGrain("test1");
        Grain g2 = s.getGrain("test2");

        assertAll(
                () -> assertEquals(3, s.getGrains().size()),
                () -> assertEquals(2, g1.getElements(SequenceElement.class).size()),
                () -> assertEquals(2, g2.getElements(SequenceElement.class).size()),
                () -> assertNotNull(g1.getElement("s1", SequenceElement.class)),
                () -> assertNotNull(g1.getElement("s2", SequenceElement.class)),
                () -> assertNotNull(g2.getElement("s3", SequenceElement.class)),
                () -> assertNotNull(g2.getElement("s4", SequenceElement.class))
        );

    }

    @Test
    void testNestedDirStructure() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "nested_dir_structure"
        );

        testMultipleFilesSchema(s);
    }

    private void testMultipleFilesSchema(AbstractScore s) throws Exception {
        Grain g = s.getGrain("test");

        assertAll(
                () -> assertEquals(2, s.getGrains().size()), ///One system grain and one is our
                () -> assertEquals(2, g.getElements(SequenceElement.class).size()),
                () -> assertNotNull(g.getElement("s1", SequenceElement.class)),
                () -> assertNotNull(g.getElement("s2", SequenceElement.class))
        );
    }

}
