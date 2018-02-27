package ru.curs.celesta.score;

import java.io.File;

public final class ResourceUtil {

    private ResourceUtil() {
        throw new AssertionError();
    }

    public static String getResourcePath(Class c, String relativePath) {
        return c.getResource(relativePath).getPath();
    }

    public static File getResourceAsFile(Class c, String relativePath) {
        String path = getResourcePath(c, relativePath);
        return new File(path);
    }
}
