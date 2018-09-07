package ru.curs.celesta.score;

import ru.curs.celesta.CelestaException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Базовый класс для элементов гранулы (таблиц, индексов и представлений).
 */
public abstract class GrainElement extends NamedElement {

    /**
     * Гранула, к которой относится данный элемент.
     */
    private final GrainPart grainPart;

    private final List<GrainElementReference> references = new ArrayList<>();
    private final List<GrainElement> referenced = new ArrayList<>();

    public GrainElement(GrainPart gp, String name) throws ParseException {
        super(name, gp.getGrain().getScore().getIdentifierParser());
        grainPart = gp;
    }

    /**
     * Возвращает гранулу, к которой относится элемент.
     */
    public final Grain getGrain() {
        return this.grainPart.getGrain();
    }

    public final GrainPart getGrainPart() {
        return grainPart;
    }

    abstract void save(PrintWriter bw) throws IOException;

    /**
     * Возвращает Celesta-SQL представление объекта.
     *
     * @throws IOException ошибка ввода-вывода при сохранении.
     */
    public String getCelestaSQL() throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter bw = new PrintWriter(sw);
        save(bw);
        bw.flush();
        return sw.toString();
    }

    public void addReference(GrainElementReference reference) {
        Grain grain = getGrain().getScore().getGrains().get(reference.getGrainName());

        GrainElement grainElement;

        try {
            grainElement = grain == null
                    ? null
                    : grain.getElement(reference.getName(), reference.getGrainElementClass());
        } catch (CelestaException e) {
            grainElement = null;
        }

        if (this == grainElement)
            return;

        GrainElementReference thisAsReference = new GrainElementReference(
                getGrain().getName(), getName(), getClass(), null
        );

        if (grainElement != null && grainElement.references.contains(thisAsReference)) {
            throw new CelestaException(
                    "Cycle reference detected between %s.%s and %s.%s",
                    this.getGrainPart().getGrain().getName(), this.getName(),
                    reference.getGrainName(), reference.getName()
            );
        }

        this.references.add(reference);
    }

    public void resolveReferences() {
        this.references.stream()
                .filter(GrainElementReference::isNotResolved)
                .forEach(GrainElementReference::resolve);
    }

    public List<GrainElementReference> getReferences() {
        return references;
    }

    public List<GrainElement> getReferenced() {
        return referenced;
    }
}
