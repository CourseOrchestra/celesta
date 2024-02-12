package ru.curs.celesta.score;

/**
 * Native SQL holder for a grain part.
 */
public final class NativeSqlElement {

    private final GrainPart grainPart;
    private final String sql;

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
