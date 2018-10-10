package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class SequenceElement extends GrainElement {

    private static final String DUPLICATE_ENTRANCE_TEMPLATE = "Duplicate entrance of %s was detected for sequence %s";

    private final Map<Argument, Object> arguments = new LinkedHashMap<>();

    SequenceElement(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name);
        getGrain().addElement(this);
    }

    void startWith(Long startWith) throws ParseException {
        if (arguments.putIfAbsent(Argument.START_WITH, startWith) != null) {
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.START_WITH, getName())
            );
        }
    }

    void incrementBy(Long incrementBy) throws ParseException {
        if (incrementBy == 0) {
            throw new ParseException(
                    String.format("Sequence %s has illegal value 0 for INCREMENT BY expression.", getName())
            );
        }
        if (arguments.putIfAbsent(Argument.INCREMENT_BY, incrementBy) != null) {
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.INCREMENT_BY, getName())
            );
        }
    }

    void minValue(Long minValue) throws ParseException {
        if (arguments.containsKey(Argument.MAXVALUE) && (Long)arguments.get(Argument.MAXVALUE) <= minValue)
            throw new ParseException(
                    String.format("MINVALUE for sequence %s must be less than MAXVALUE", getName())
            );

        if (arguments.putIfAbsent(Argument.MINVALUE, minValue) != null)
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.MINVALUE, getName())
            );
    }

    void maxValue(Long maxValue) throws ParseException {
        if (arguments.containsKey(Argument.MINVALUE) && (Long)(arguments.get(Argument.MINVALUE)) >= maxValue)
            throw new ParseException(
                    String.format("MAXVALUE for sequence %s must be greater than MINVALUE", getName())
            );

        if (arguments.putIfAbsent(Argument.MAXVALUE, maxValue) != null)
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.MINVALUE, getName())
            );
    }

    void setIsCycle(Boolean isCycle) throws ParseException {
        if (arguments.putIfAbsent(Argument.CYCLE, isCycle) != null)
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.CYCLE, getName())
            );
    }


    @Override
    void save(PrintWriter bw) throws IOException {
        Grain.writeCelestaDoc(this, bw);
        bw.printf("CREATE SEQUENCE %s ", getName());


        if (hasArgument(Argument.START_WITH)) {
            bw.printf("START WITH %s ", getArgument(Argument.START_WITH));
        }

        if (hasArgument(Argument.INCREMENT_BY)) {
            bw.printf("INCREMENT BY %s ", getArgument((Argument.INCREMENT_BY)));
        }

        if (hasArgument(Argument.MINVALUE)) {
            bw.printf("MINVALUE %s ", getArgument(Argument.MINVALUE));
        }

        if (hasArgument(Argument.MAXVALUE)) {
            bw.printf("MAXVALUE %s ", getArgument(Argument.MAXVALUE));
        }

        if (hasArgument(Argument.CYCLE) && (Boolean)getArgument(Argument.CYCLE)) {
            bw.write("CYCLE ");
        }

        bw.println(";");
        bw.println();
    }

    void finalizeParsing() throws ParseException {
        arguments.putIfAbsent(Argument.START_WITH, 1L);
        arguments.putIfAbsent(Argument.INCREMENT_BY, 1L);

        Long startWith = (Long) getArgument(Argument.START_WITH);
        Long incrementBy = (Long) getArgument(Argument.INCREMENT_BY);

        if (!hasArgument(Argument.MINVALUE)) {
            minValue(startWith);
        }

        Long minValue = (Long) getArgument(Argument.MINVALUE);
        if (startWith < minValue) {
            throw new ParseException(
                    String.format("MINVALUE for sequence %s can't be greater than START WITH", getName())
            );
        }

        if (!hasArgument(Argument.MAXVALUE)) {
            maxValue(Long.MAX_VALUE);
        }

        Long maxValue = (Long) getArgument(Argument.MAXVALUE);
        if (startWith > maxValue) {
            throw new ParseException(
                    String.format("MAXVALUE for sequence %s must be greater or equals START WITH", getName())
            );
        }

        if (!hasArgument(Argument.CYCLE)) {
            setIsCycle(false);
        }

        if (incrementBy < 0) {
            if (startWith > 0 && (startWith + incrementBy) < minValue) {
                throw new ParseException(
                        String.format("Sum of arguments START WITH AND INCREMENT BY must be greater or equals MINVALUE " +
                                "for sequence %s  in case of descending increment", getName())
                );
            }

            if (Math.abs(incrementBy) >= Math.abs((maxValue - minValue)))
                throw new ParseException(
                        String.format("Absolute value of 'INCREMENT BY' must be less than absolute value of subtraction of MAXVALUE and MINVALUE " +
                                "for sequence %s in case of descending increment", getName())
                );

        }

    }


    public Map<Argument, Object> getArguments() {
        return arguments;
    }

    public boolean hasArgument(Argument argument) {
        return arguments.containsKey(argument);
    }

    public Object getArgument(Argument argument) {
        return arguments.get(argument);
    }


    public enum Argument {
        START_WITH("START WITH", "START WITH %s "),
        INCREMENT_BY("INCREMENT BY", "INCREMENT BY %s "),
        MINVALUE("MINVALUE", "MINVALUE %s "),
        MAXVALUE("MAXVALUE", "MAXVALUE %s "),
        CYCLE("CYCLE", "CYCLE ");

        private final String type;
        private final String sqlTemplate;

        Argument(String type, String sqlTemplate) {
            this.type = type;
            this.sqlTemplate = sqlTemplate;
        }

        public String getSql(Object value) {

            if (this == CYCLE && Objects.equals(false, value))
                return "";

            return String.format(sqlTemplate, value);
        }


        @Override
        public String toString() {
            return type;
        }
    }
}
