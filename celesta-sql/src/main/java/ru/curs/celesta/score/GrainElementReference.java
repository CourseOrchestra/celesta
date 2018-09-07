package ru.curs.celesta.score;

import java.util.Objects;
import java.util.function.Consumer;

public class GrainElementReference {
    private final String grainName;
    private final String name;
    private final  Class<? extends GrainElement> grainElementClass;
    private final Consumer<GrainElementReference> resolver;
    private boolean resolved;

    public GrainElementReference(String grainName, String name, Class<? extends GrainElement> grainElementClass,
                                 Consumer<GrainElementReference> resolver) {
        this.grainName = grainName;
        this.name = name;
        this.grainElementClass = grainElementClass;
        this.resolver = resolver;
    }

    public String getGrainName() {
        return grainName;
    }

    public String getName() {
        return name;
    }

    public Class<? extends GrainElement> getGrainElementClass() {
        return grainElementClass;
    }

    public boolean isNotResolved() {
        return !resolved;
    }

    public void resolve() {
        this.resolver.accept(this);
        this.resolved = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GrainElementReference that = (GrainElementReference) o;
        return Objects.equals(grainName, that.grainName) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grainName, name);
    }
}
