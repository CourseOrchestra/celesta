package ru.curs.celesta.score.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Score resource abstraction. Its implementation may point to a physical file or a resource in
 * a JAR-file.
 * 
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-02-23
 */
public interface Resource {
    /**
     * Returns input stream for the resource.
     *
     * @return
     * @throws IOException
     */
    InputStream getInputStream() throws IOException;
    
    /**
     * Returns output stream for the resource if it is writable, {@code null} otherwise.
     *
     * @return
     * @throws IOException
     */
    OutputStream getOutputStream() throws IOException;

    /**
     * Checks if the passed-in resource is child of {@code this} resource.
     * @param childResource
     * @return
     */
    boolean contains(Resource childResource);
    
    /**
     * Returns relative path from {@code this} resource to the child one, or
     * {@code null} if such path doesn't exist.
     *
     * @param childResource
     * @return
     */
    String getRelativePath(Resource childResource);

}
