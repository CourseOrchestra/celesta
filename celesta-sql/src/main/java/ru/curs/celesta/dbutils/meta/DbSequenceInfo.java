package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.SequenceElement;

import static ru.curs.celesta.score.SequenceElement.Argument.*;

import java.util.Objects;

public class DbSequenceInfo {
    private long incrementBy;
    private long minValue;
    private long maxValue;
    private boolean isCycle;

    public long getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(long incrementBy) {
        this.incrementBy = incrementBy;
    }

    public long getMinValue() {
        return minValue;
    }

    public void setMinValue(long minValue) {
        this.minValue = minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    public boolean isCycle() {
        return isCycle;
    }

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
