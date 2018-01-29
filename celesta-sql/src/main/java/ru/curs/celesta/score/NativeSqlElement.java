package ru.curs.celesta.score;

public class NativeSqlElement {

    private GrainPart grainPart;
    private String sql;

    public NativeSqlElement(GrainPart grainPart, String sql) {
        this.grainPart = grainPart;
        this.sql = sql;
    }

    public GrainPart getGrainPart() {
        return grainPart;
    }

    public String getSql() {
        return sql;
    }
}
