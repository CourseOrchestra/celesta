package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.SequenceElement;

import static ru.curs.celesta.score.SequenceElement.Argument.*;

import java.util.Objects;

/**
 * Information on sequence taken from the database.
 */
public final class DbSequenceInfo {
    private long incrementBy;
    private long minValue;
    private long maxValue;
    private boolean isCycle;

    /**
     * Returns 'increment by' value of the sequence.
     *
     * @return
     */
    public long getIncrementBy() {
        return incrementBy;
    }

    /**
     * Sets 'increment by' value of the sequence.
     *
     * @param incrementBy  'increment by' value
     */
    public void setIncrementBy(long incrementBy) {
        this.incrementBy = incrementBy;
    }

    /**
     * Returns minimal value of the sequence.
     *
     * @return
     */
    public long getMinValue() {
        return minValue;
    }

    /**
     * Sets minimal value of the sequence.
     *
     * @param minValue  minimal value
     */
    public void setMinValue(long minValue) {
        this.minValue = minValue;
    }

    /**
     * Returns maximal value of the sequence.
     *
     * @return
     */
    public long getMaxValue() {
        return maxValue;
    }

    /**
     * Sets maximal value of the sequence.
     *
     * @param maxValue  maximal value
     */
    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * Whether the sequence is cycled, i.e. can start again from the min. value.
     *
     * @return  {@code true} - cycled, otherwise - {@code false}. Default is {@code false}
     */
    public boolean isCycle() {
        return isCycle;
    }

    /**
     * Sets if the sequence is cycled.
     *
     * @param cycle  {@code true} - cycled, otherwise - {@code false}.
     */
    public void setCycle(boolean cycle) {
        isCycle = cycle;
    }

    public boolean reflects(SequenceElement s) {
        return !Objects.equals(incrementBy, s.getArgument(INCREMENT_BY))
                || !Objects.equals(minValue, s.getArgument(MINVALUE))
                || !Objects.equals(maxValue, s.getArgument(MAXVALUE))
                || !Objects.equals(isCycle, s.getArgument(CYCLE));
    }

}
