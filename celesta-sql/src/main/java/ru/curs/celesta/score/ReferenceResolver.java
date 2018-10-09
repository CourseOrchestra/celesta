package ru.curs.celesta.score;

import ru.curs.celesta.exception.CelestaParseException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ReferenceResolver {

    public static final String CYCLE_REFERENCE_ERROR_MESSAGE_TEMPLATE = "Cycle reference detected on chain: %s";

    private final AbstractScore score;


    ReferenceResolver(AbstractScore score) {
        this.score = score;
    }

    void resolveReferences(Grain g) throws ParseException {
        for (Table t : g.getElements(Table.class).values()) {
            GrainElementReference tAsReference = new GrainElementReference(
                    g.getName(), t.getName(), Table.class, null
            );
            LinkedList<GrainElementReference> referenceChain = new LinkedList<>();
            referenceChain.add(tAsReference);


            if (hasCycleDependency(t.getReferences(), referenceChain)) {
                List<GrainElementReference> cycleChain = new ArrayList<>(referenceChain);
                cycleChain.add(cycleChain.get(0));
                throw new CelestaParseException(
                        CYCLE_REFERENCE_ERROR_MESSAGE_TEMPLATE,
                        cycleChain.stream()
                                .map((r) -> String.format("%s.%s", r.getGrainName(), r.getName()))
                                .collect(Collectors.joining(" -> "))
                );
            }

            t.resolveReferences();
        }
    }


    private boolean hasCycleDependency(List<GrainElementReference> references,
                                       LinkedList<GrainElementReference> referenceChain)
            throws ParseException {
        for (GrainElementReference grainElementReference : references) {
            if (referenceChain.contains(grainElementReference)) {
                // Preparing of real cycle chain
                Iterator<GrainElementReference> it = referenceChain.iterator();

                while (it.hasNext()) {
                    GrainElementReference referenceFromChain = it.next();
                    if (grainElementReference.equals(referenceFromChain)) {
                        break;
                    } else {
                        it.remove();
                    }
                }

                return true;
            }

            Grain g = this.score.getGrain(grainElementReference.getGrainName());
            GrainElement ge = g.getElement(
                    grainElementReference.getName(),
                    grainElementReference.getGrainElementClass()
            );


            LinkedList<GrainElementReference> newReferenceChain = new LinkedList<>(referenceChain);
            newReferenceChain.add(grainElementReference);

            boolean nestedResult = this.hasCycleDependency(ge.getReferences(), newReferenceChain);

            if (nestedResult) {
                referenceChain.clear();
                referenceChain.addAll(newReferenceChain);
                return true;
            }
        }

        return false;
    }
}
