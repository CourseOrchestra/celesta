package ru.curs.celesta.dbutils.meta;

public class DbSequenceInfo {
    private String name;
    private long incrementBy;
    private long minValue;
    private long maxValue;
    private boolean isCycle;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

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
}
