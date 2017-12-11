package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class Sequence extends GrainElement {

    private static final String DUPLICATE_ENTRANCE_TEMPLATE = "Duplicate entrance of %s was detected for sequence %s";

    private final Map<Argument, Object> arguments = new LinkedHashMap<>();

    Sequence(Grain g, String name) throws ParseException {
        super(g, name);
        g.addElement(this);
    }

    void startWith(Integer startWith) throws ParseException {
        if (arguments.putIfAbsent(Argument.START_WITH, startWith) != null)
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.START_WITH, getName())
            );
    }

    void incrementBy(Integer incrementBy) throws ParseException {
        if (incrementBy == 0)
            throw new ParseException(
                    String.format("Sequence %s has illegal value 0 for INCREMENT BY expression.", getName())
            );
        if (arguments.putIfAbsent(Argument.INCREMENT_BY, incrementBy) != null)
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.INCREMENT_BY, getName())
            );
    }

    void minValue(Integer minValue) throws ParseException {
        if (arguments.containsKey(Argument.MAXVALUE) && (Integer)arguments.get(Argument.MAXVALUE) <= minValue)
            throw new ParseException(
                    String.format("MINVALUE for sequence %s must be less than MAXVALUE", getName())
            );

        if (arguments.putIfAbsent(Argument.MINVALUE, minValue) != null)
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.MINVALUE, getName())
            );
    }

    void maxValue(Integer maxValue) throws ParseException {
        if (arguments.containsKey(Argument.MINVALUE) && (Integer)(arguments.get(Argument.MINVALUE)) >= maxValue)
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
    void save(BufferedWriter bw) throws IOException {
        Grain.writeCelestaDoc(this, bw);
        bw.write(String.format("CREATE SEQUENCE %s ", getName()));


        if (hasArgument(Argument.START_WITH)) {
            bw.write(String.format("START WITH %s ", getArgument(Argument.START_WITH)));
        }

        if (hasArgument(Argument.INCREMENT_BY)) {
            bw.write(String.format("INCREMENT BY %s ", getArgument((Argument.INCREMENT_BY))));
        }

        if (hasArgument(Argument.MINVALUE)) {
            bw.write(String.format("MINVALUE %s ", getArgument(Argument.MINVALUE)));
        }

        if (hasArgument(Argument.MAXVALUE)) {
            bw.write(String.format("MAXVALUE %s ", getArgument(Argument.MAXVALUE)));
        }

        if (hasArgument(Argument.CYCLE) && (Boolean)getArgument(Argument.CYCLE)) {
            bw.write("CYCLE ");
        }

        bw.write(";");
        bw.newLine();
        bw.newLine();
    }

    void finalizeParsing() throws ParseException {
        arguments.putIfAbsent(Argument.START_WITH, 1);
        arguments.putIfAbsent(Argument.INCREMENT_BY, 1);

        Integer startWith = (Integer) getArgument(Argument.START_WITH);
        Integer incrementBy = (Integer) getArgument(Argument.INCREMENT_BY);


        if (hasArgument(Argument.MINVALUE)) {
            Integer minValue = (Integer) getArgument(Argument.MINVALUE);
            if (startWith < minValue) {
                throw new ParseException(
                        String.format("MINVALUE for sequence %s can't be greater than START WITH", getName())
                );
            }
        }
        if (hasArgument(Argument.MAXVALUE)) {
            Integer maxValue = (Integer) getArgument(Argument.MAXVALUE);
            if (startWith > maxValue) {
                throw new ParseException(
                        String.format("MAXVALUE for sequence %s must be greater or equals START WITH", getName())
                );
            }
        }


        if (incrementBy > 0) {
            if (hasArgument(Argument.CYCLE) && !hasArgument(Argument.MAXVALUE))
                throw new ParseException(
                        String.format("MAXVALUE for sequence %s must be specified in case of ascending increment and cycle", getName())
                );
        }

        if (incrementBy < 0) {
            if (!hasArgument(Argument.MAXVALUE))
                throw new ParseException(
                        String.format("MAXVALUE for sequence %s must be specified in case of descending increment", getName())
                );


            if (hasArgument(Argument.MINVALUE)) {
                Integer minValue = (Integer) getArgument(Argument.MINVALUE);
                Integer maxValue = (Integer) getArgument(Argument.MAXVALUE);

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

        public String getSql(Object... values) {
            return String.format(sqlTemplate, values);
        }


        @Override
        public String toString() {
            return type;
        }
    }
}
