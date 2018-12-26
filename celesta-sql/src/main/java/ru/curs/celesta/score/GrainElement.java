package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

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

    abstract void save(PrintWriter bw) throws IOException;

    /**
     * Returns Celesta-SQL representation of object.
     *
     * @return
     * @throws IOException  IO exception when saving
     */
    public String getCelestaSQL() throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter bw = new PrintWriter(sw);
        save(bw);
        bw.flush();
        return sw.toString();
    }

}
