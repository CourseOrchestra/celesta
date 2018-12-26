package ru.curs.celesta.score;

/**
 * Naitive SQL holder for a grain part.
 */
public final class NativeSqlElement {

    private GrainPart grainPart;
    private String sql;

    public NativeSqlElement(GrainPart grainPart, String sql) {
        this.grainPart = grainPart;
        this.sql = sql;
    }

    /**
     * Returns grain part.
     *
     * @return
     */
    public GrainPart getGrainPart() {
        return grainPart;
    }

    /**
     * Returns native SQL.
     *
     * @return
     */
    public String getSql() {
        return sql;
    }

}
