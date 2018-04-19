package ru.curs.celesta.plugin;

import org.apache.maven.plugins.annotations.Parameter;

import java.util.Objects;

public class ScoreProperties {

    @Parameter(property = "path", required = true)
    private String path;

    @Parameter(property = "rootPackage", required = true)
    private String rootPackage;

    public ScoreProperties() {
    }

    public ScoreProperties(String path, String rootPackage) {
        this.path = path;
        this.rootPackage = rootPackage;
    }

    public String getPath() {
        return path;
    }

    public String getRootPackage() {
        return rootPackage;
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
                    && Objects.equals(this.path, that.path)
                    && Objects.equals(this.rootPackage, that.rootPackage);
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return this.path.hashCode() + this.rootPackage.hashCode();
    }
}
