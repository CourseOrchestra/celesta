package ru.curs.celesta.score;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Sequence object in metadata.
 */
public final class SequenceElement extends GrainElement {

    private static final String DUPLICATE_ENTRANCE_TEMPLATE = "Duplicate entrance of %s was detected for sequence %s";

    private final Map<Argument, Object> arguments = new LinkedHashMap<>();

    SequenceElement(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name);
        getGrain().addElement(this);
    }

    /**
     * Sets value for {@code START_WITH} argument of the sequence.
     *
     * @param startWith  value of {@code START_WITH} argument
     * @throws ParseException
     */
    void startWith(Long startWith) throws ParseException {
        if (arguments.putIfAbsent(Argument.START_WITH, startWith) != null) {
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.START_WITH, getName())
            );
        }
    }

    /**
     * Sets value for {@code INCREMENT_BY} argument of the sequence.
     *
     * @param incrementBy  value of {@code INCREMENT_BY} argument
     * @throws ParseException
     */
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

    /**
     * Sets value for {@code MINVALUE} argument of the sequence.
     *
     * @param minValue  value of {@code MINVALUE} argument
     * @throws ParseException
     */
    void minValue(Long minValue) throws ParseException {
        if (arguments.containsKey(Argument.MAXVALUE) && (Long) arguments.get(Argument.MAXVALUE) <= minValue) {
            throw new ParseException(
                    String.format("MINVALUE for sequence %s must be less than MAXVALUE", getName())
            );
        }

        if (arguments.putIfAbsent(Argument.MINVALUE, minValue) != null) {
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.MINVALUE, getName())
            );
        }
    }

    /**
     * Sets value for {@code MAXVALUE} argument of the sequence.
     *
     * @param maxValue value of {@code MAXVALUE} argument
     * @throws ParseException
     */
    void maxValue(Long maxValue) throws ParseException {
        if (arguments.containsKey(Argument.MINVALUE) && ((Long) arguments.get(Argument.MINVALUE)) >= maxValue) {
            throw new ParseException(
                    String.format("MAXVALUE for sequence %s must be greater than MINVALUE", getName())
            );
        }

        if (arguments.putIfAbsent(Argument.MAXVALUE, maxValue) != null) {
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.MINVALUE, getName())
            );
        }
    }

    /**
     * Sets value for {@code CYCLE} argument of the sequence.
     *
     * @param isCycle  value of {@code CYCLE} argument
     * @throws ParseException
     */
    void setIsCycle(Boolean isCycle) throws ParseException {
        if (arguments.putIfAbsent(Argument.CYCLE, isCycle) != null) {
            throw new ParseException(
                    String.format(DUPLICATE_ENTRANCE_TEMPLATE, Argument.CYCLE, getName())
            );
        }
    }

    void finalizeParsing() throws ParseException {
        arguments.putIfAbsent(Argument.START_WITH, 1L);
        arguments.putIfAbsent(Argument.INCREMENT_BY, 1L);

        Long startWith = getStartWith();
        Long incrementBy = getIncrementBy();

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
                        String.format("Sum of arguments START WITH AND INCREMENT BY must be greater or equals MINVALUE "
                                + "for sequence %s  in case of descending increment", getName())
                );
            }

            if (Math.abs(incrementBy) >= Math.abs(maxValue - minValue)) {
                throw new ParseException(
                        String.format("Absolute value of 'INCREMENT BY' must be less than "
                                + "absolute value of subtraction of MAXVALUE and MINVALUE "
                                + "for sequence %s in case of descending increment", getName())
                );
            }
        }

    }

    /**
     * Returns a map of all arguments with values for the sequence.
     *
     */
    public Map<Argument, Object> getArguments() {
        return arguments;
    }

    /**
     * Whether the sequence contains the specified argument.
     *
     * @param argument  argument
     */
    public boolean hasArgument(Argument argument) {
        return arguments.containsKey(argument);
    }

    /**
     * Returns value of the specified argument.
     *
     * @param argument  argument
     */
    private Object getArgument(Argument argument) {
        return arguments.get(argument);
    }

    /**
     * Returns STARTS WITH parameter for this SEQUENCE.
     */
    public Long getStartWith() {
        return (Long) arguments.get(Argument.START_WITH);
    }

    /**
     * Returns MINVALUE parameter for this SEQUENCE.
     */
    public Long getMinValue() {
        return (Long) arguments.get(Argument.MINVALUE);
    }

    /**
     * Returns MAXVALUE parameter for this SEQUENCE.
     */
    public Long getMaxValue() {
        return (Long) arguments.get(Argument.MAXVALUE);
    }

    /**
     * Returns INCREMENT BY parameter for this SEQUENCE.
     */
    public Long getIncrementBy() {
        return (Long) arguments.get(Argument.INCREMENT_BY);
    }

    /**
     * Returns CYCLE parameter for this SEQUENCE.
     */
    public Boolean isCycle() {
        return (Boolean) arguments.get(Argument.CYCLE);
    }

    /**
     * Sequence arguments.
     */
    public enum Argument {
        /**
         * {@code START_WITH} argument.
         */
        START_WITH("START WITH", "START WITH %s "),
        /**
         * {@code INCREMENT_BY} argument.
         */
        INCREMENT_BY("INCREMENT BY", "INCREMENT BY %s "),
        /**
         * {@code MINVALUE} argument.
         */
        MINVALUE("MINVALUE", "MINVALUE %s "),
        /**
         * {@code MAXVALUE} argument.
         */
        MAXVALUE("MAXVALUE", "MAXVALUE %s "),
        /**
         * {@code CYCLE} argument.
         */
        CYCLE("CYCLE", "CYCLE ");

        private final String type;
        private final String sqlTemplate;

        Argument(String type, String sqlTemplate) {
            this.type = type;
            this.sqlTemplate = sqlTemplate;
        }

        /**
         * Returns SQL for argument with the specified {@code value}.
         *
         * @param value  argument value
         */
        public String getSql(Object value) {

            if (this == CYCLE && Objects.equals(false, value)) {
                return "";
            }

            return String.format(sqlTemplate, value);
        }

        @Override
        public String toString() {
            return type;
        }
    }

}
