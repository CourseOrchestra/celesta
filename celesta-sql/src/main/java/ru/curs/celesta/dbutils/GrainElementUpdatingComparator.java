package ru.curs.celesta.dbutils;

import ru.curs.celesta.score.*;

import java.util.Comparator;

public class GrainElementUpdatingComparator implements Comparator<GrainElement> {


    private final AbstractScore score;

    public GrainElementUpdatingComparator(AbstractScore score) {
        this.score = score;
    }

    @Override
    public int compare(GrainElement o1, GrainElement o2) {

        if (this.firstIsSequenceAndSecondIsNot(o1, o2)) {
            // TODO: Temporary if-else-if. Will be removed with injection of sequences as references (issue is needed)
            return -1;
        } else if (this.firstIsSequenceAndSecondIsNot(o2, o1)) {
            return 1;
        }

        if (this.firstIsTableAndSecondIsNot(o1, o2)) {
            // TODO: Temporary if-else-if. Will be removed with injection of tables as references (issue is needed)
            return -1;
        } else if (this.firstIsTableAndSecondIsNot(o2, o1)) {
            return 1;
        }


        if (o1.getClass().equals(o2.getClass())) {
            if (o1.getReferenced().size() > 0 && o2.getReferenced().isEmpty()) {
                return -1;
            } else if (o2.getReferenced().size() > 0 && o1.getReferenced().isEmpty()) {
                return 1;
            }

            if (o1.getReferences().size() > 0 && o2.getReferences().isEmpty()) {
                return 1;
            } else if (o2.getReferences().size() > 0 && o1.getReferences().isEmpty()) {
                return -1;
            }
        }

        if (this.firstDependsOnSecond(o1, o2)) {
            return 1;
        } else if (this.firstDependsOnSecond(o2, o1)) {
            return -1;
        }

        if (this.firstIsIndexAndSecondIsNot(o1, o2)) {
            // TODO: Temporary if-else-if. Will be removed with injection of tables as references (issue is needed)
            return -1;
        } else if (this.firstIsIndexAndSecondIsNot(o2, o1)) {
            return 1;
        }

        return 0;
    }

    private boolean firstDependsOnSecond(GrainElement first, GrainElement second) {

        for (GrainElementReference reference: first.getReferences()) {
            Grain grain = score.getGrain(reference.getGrainName());
            GrainElement ge = grain.getElement(reference.getName(), reference.getGrainElementClass());

            if (ge == second || this.firstDependsOnSecond(ge, second)) {
                return true;
            }
        }

        return false;
    }

    private boolean firstIsSequenceAndSecondIsNot(GrainElement first, GrainElement second) {
        return first instanceof SequenceElement && !(second instanceof SequenceElement);
    }

    private boolean firstIsTableAndSecondIsNot(GrainElement first, GrainElement second) {
        return first instanceof Table && !(second instanceof Table);
    }

    private boolean firstIsIndexAndSecondIsNot(GrainElement first, GrainElement second) {
        return first instanceof Index && !(second instanceof Index);
    }

}
