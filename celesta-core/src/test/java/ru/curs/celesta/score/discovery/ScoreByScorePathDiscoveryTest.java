package ru.curs.celesta.score.discovery;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.Namespace;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class ScoreByScorePathDiscoveryTest {

    private final ScoreByScorePathDiscovery scoreDiscovery = new ScoreByScorePathDiscovery("foo/score");

    @Test
    void testGetNamespaceFromPath() {
        File f = new File("");
        Namespace ns = scoreDiscovery.getNamespaceFromPath(f.toPath());
        assertSame(Namespace.DEFAULT, ns);

        f = new File("/table.sql");
        ns = scoreDiscovery.getNamespaceFromPath(f.toPath());
        assertSame(Namespace.DEFAULT, ns);

        f = new File("data/table/Table.sql");
        ns = scoreDiscovery.getNamespaceFromPath(f.toPath());
        assertEquals("data.table", ns.getValue());
    }

}
