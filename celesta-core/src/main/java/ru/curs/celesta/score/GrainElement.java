package ru.curs.celesta.score;

/**
 * Base class for grain elements (tables, indices and views).
 */
public abstract class GrainElement extends NamedElement {

    /**
     * Grain that current element belongs to.
     */
    private final GrainPart grainPart;

    public GrainElement(GrainPart gp, String name) throws ParseException {
        super(name, gp.getGrain().getScore().getIdentifierParser());
        grainPart = gp;
    }

    /**
     * Returns grain that the element belongs to.
     *
     * @return
     */
    public final Grain getGrain() {
        return this.grainPart.getGrain();
    }

    public final GrainPart getGrainPart() {
        return grainPart;
    }

}
