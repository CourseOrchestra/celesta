package ru.curs.celesta;

import ru.curs.celesta.score.GrainPart;

/**
 * The exception that occurs when re-parsing the same {@link GrainPart}.
 */
public class RepeatedParseException extends RuntimeException {

    public RepeatedParseException(GrainPart grainPart) {
        super(String.format("Repeated parsing of %s was detected.", grainPart.getSource()));
    }

}
