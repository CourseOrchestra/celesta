package ru.curs.celesta.score.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.curs.celesta.score.Namespace;

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
     * @throws IOException  when input stream cannot be provided.
     */
    InputStream getInputStream() throws IOException;

    /**
     * Returns output stream for the resource if it is writable, {@code null} otherwise.
     *
     * @return
     * @throws IOException  when output stream cannot be provided.
     */
    default OutputStream getOutputStream() throws IOException {
        return null;
    }

    /**
     * Deletes physical resource that {@code this} resource points at.
     *
     * @return {@code true} if successfully deleted or {@code false} otherwise.
     * @throws IOException  when an error happened during deletion.
     */
    default boolean delete() throws IOException {
        return false;
    }

    /**
     * Checks if the passed-in resource is a child of {@code this} resource.
     *
     * @param childResource  the checked resource.
     * @return
     */
    default boolean contains(Resource childResource) {
        return false;
    }

    /**
     * Returns relative path from {@code this} resource to the child one, or
     * {@code null} if such path doesn't exist.
     *
     * @param childResource  child resource of {@code this} resource.
     * @return
     */
    default String getRelativePath(Resource childResource) {
        return null;
    }

    /**
     * Creates a resource relative to this resource.
     *
     * @param namespace  name space path relative to {@code this} resource.
     * @return
     * @throws IOException  when resource creation failed.
     */
    default Resource createRelative(Namespace namespace) throws IOException {
        return createRelative(namespace.getValue().replace('.', '/'), Namespace.DEFAULT);
    }

    /**
     * Creates a resource relative to this resource.
     *
     * @param relativePath  path relative to {@code this} resource.
     * @return
     * @throws IOException  when resource creation failed.
     */
    default Resource createRelative(String relativePath) throws IOException {
        return createRelative(relativePath, Namespace.DEFAULT);
    }

    /**
     * Creates a resource relative to this resource.
     *
     * @param relativePath  path relative to {@code this} resource.
     * @param namespace  name space of the created resource.
     * @return
     * @throws IOException  when resource creation failed.
     */
    Resource createRelative(String relativePath, Namespace namespace) throws IOException;

    /**
     * Returns namespace of current resource.
     *
     * @return
     */
    Namespace getNamespace();

}
