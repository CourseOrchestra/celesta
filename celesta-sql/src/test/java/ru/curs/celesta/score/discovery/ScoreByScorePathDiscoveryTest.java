package ru.curs.celesta.score.discovery;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.Test;

import ru.curs.celesta.score.Namespace;

public class ScoreByScorePathDiscoveryTest {
    
    private final ScoreByScorePathDiscovery scoreDiscovery = new ScoreByScorePathDiscovery("foo/score"); 
    
    @Test
    void testGetNamespaceFromPath() {
        File f = new File("/table.sql");
        Namespace ns = scoreDiscovery.getNamespaceFromPath(f.toPath());
        assertNull(ns);
        
        f = new File("data/table/Table.sql");
        ns = scoreDiscovery.getNamespaceFromPath(f.toPath());
        assertEquals("data.table", ns.getValue());
    }

}
