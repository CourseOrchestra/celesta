package ru.curs.celesta.plugin;

import org.apache.maven.plugins.annotations.Parameter;

import java.util.Objects;

public class ScoreProperties {

    @Parameter(property = "path", required = true)
    private String path;

    public ScoreProperties() {
    }

    public ScoreProperties(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this == obj)
            return true;
        if (obj instanceof ScoreProperties) {
            ScoreProperties that = (ScoreProperties)obj;
            return this.hashCode() == that.hashCode()
                    && Objects.equals(this.path, that.path);
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return this.path.hashCode();
    }
}
