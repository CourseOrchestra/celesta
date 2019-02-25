package ru.curs.celesta.score.io;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

/**
 * Score resource located in a JAR-file.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-02-23
 */
public final class UrlResource implements Resource {

    private final URL url;
    private final String urlString;

    public UrlResource(URL url) {
        this.url = url;
        this.urlString = url.toString();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        URLConnection con = this.url.openConnection();
        con.setUseCaches(false);
        try {
            return con.getInputStream();
        } catch (IOException ex) {
            if (con instanceof HttpURLConnection) {
                    ((HttpURLConnection) con).disconnect();
            }
            throw ex;
        }
    }

    @Override
    public UrlResource createRelative(String relativePath) throws MalformedURLException {
        if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
        }

        return new UrlResource(new URL(this.url, relativePath));
    }

    @Override
    public String toString() {
        return urlString;
    }

    @Override
    public int hashCode() {
        return urlString.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        UrlResource other = (UrlResource) obj;

        return Objects.equals(urlString, other.urlString);
    }

}
