package ru.curs.celesta.score;

import java.io.File;

public class GrainPart {

    private final Grain grain;
    private final boolean isDefinition;

    private File sourceFile;

    public GrainPart(Grain grain, boolean isDefinition, File sourceFile) {
        this.grain = grain;
        this.isDefinition = isDefinition;
        this.sourceFile = sourceFile;

        grain.getGrainParts().add(this);
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

    public File getSourceFile() {
        return sourceFile;
    }

    public boolean isDefinition() {
        return isDefinition;
    }

}
