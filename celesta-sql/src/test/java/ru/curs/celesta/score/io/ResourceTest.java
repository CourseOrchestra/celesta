package ru.curs.celesta.score.io;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ResourceTest {

    @Test
    public void testContains() {
        Resource parent = new FileResource(new File("/parent/"));
        Resource child = new FileResource(new File("/parent/child/../child/file.test"));
        Resource nonChild = new FileResource(new File("/parent/child/../../file.test"));
        
        assertTrue(parent.contains(child));
        assertFalse(child.contains(parent));
        assertFalse(parent.contains(nonChild));
    }

    @Test
    public void testGetRelativePath() {
        Resource parent = new FileResource(new File("/parent/"));
        Resource child = new FileResource(new File("/parent/child/file.test"));
        Resource nonChild = new FileResource(new File("/parent/child/../../file.test"));
        
        String relativePath = parent.getRelativePath(child);
        assertEquals("child/file.test", relativePath);
        
        relativePath = parent.getRelativePath(nonChild);
        assertNull(relativePath);
    }
    
    @Test
    public void testCreateRelative() throws IOException {
        Resource parent = new FileResource(new File("/parent/"));
        Resource child = parent.createRelative("/child/file.test");

        assertTrue(parent.contains(child));

        parent = new UrlResource(new URL("file:/parent/"));
        child = parent.createRelative("/child/file.test");
        
        assertEquals("file:/parent/child/file.test", child.toString());
    }

}
