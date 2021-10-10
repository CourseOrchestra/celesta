package ru.curs.celesta.score.io;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.Namespace;

import java.io.File;
import java.io.IOException;
import java.net.URL;

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
        assertEquals("child" + File.separator + "file.test", relativePath);

        relativePath = parent.getRelativePath(nonChild);
        assertNull(relativePath);
    }

    @Test
    public void testCreateRelative_path() throws IOException {
        Resource parent = new FileResource(new File("/parent/"));
        Resource child = parent.createRelative("/child/file.test");

        assertTrue(parent.contains(child));

        parent = new UrlResource(new URL("file:/parent/"));
        child = parent.createRelative("/child/file.test");

        assertEquals("file:/parent/child/file.test", child.toString());

        parent = new UrlResource(new URL("file:/parent/file.test"));
        child = parent.createRelative("/child/file.test");

        assertEquals("file:/parent/child/file.test", child.toString());
    }

    @Test
    public void testCreateRelative_namespace() throws IOException {
        
        Resource parent = new FileResource(new File("/parent/"));
        Resource child = new  FileResource(new File("/parent/child"));
        
        assertEquals(parent, parent.createRelative(Namespace.DEFAULT));
        assertEquals(child, parent.createRelative(new Namespace("child")));
    }

}
