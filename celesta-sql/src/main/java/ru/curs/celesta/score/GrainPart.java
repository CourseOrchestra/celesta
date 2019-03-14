package ru.curs.celesta.score;

import ru.curs.celesta.score.io.Resource;

public final class GrainPart {

    private final Grain grain;
    private final boolean isDefinition;

    private final Resource source;

    public GrainPart(Grain grain, boolean isDefinition, Resource source) {
        this.grain = grain;
        this.isDefinition = isDefinition;
        this.source = source;

        grain.addGrainPart(this);
    }

    void setCelestaDocLexem(String celestaDoc) throws ParseException {
        this.grain.setCelestaDocLexem(celestaDoc);
    }

    public void setVersion(String version) throws ParseException {
        this.grain.setVersion(version);
    }

    public void setAutoupdate(boolean isAutoupdate) {
        this.grain.setAutoupdate(isAutoupdate);
    }

    void modify() throws ParseException {
        this.grain.modify();
    }

    Grain getGrain() {
        return this.grain;
    }

    /**
     * Returns name space of {@code this} grain part.
     *
     * @return
     */
    public Namespace getNamespace() {
        return source != null ? source.getNamespace() : Namespace.DEFAULT;
    }

    /**
     * Returns resource the grain part was loaded from.
     *
     * @return
     */
    public Resource getSource() {
        return source;
    }

    public boolean isDefinition() {
        return isDefinition;
    }

}
