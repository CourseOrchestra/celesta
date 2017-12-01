package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Optional;

public class Sequence extends GrainElement {

    private Optional<Integer> startWith = Optional.empty();
    private Optional<Integer> incrementBy = Optional.empty();
    private Optional<Integer> minValue = Optional.empty();
    private Optional<Integer> maxValue = Optional.empty();
    private Optional<Boolean> isCycle = Optional.empty();


    Sequence(Grain g, String name) throws ParseException {
        super(g, name);
    }

    void setCelestaDocLexem(String doc) {}

    void startWith(Integer startWith) {
        this.startWith = Optional.of(startWith);
    }

    void incrementBy(Integer incrementBy) throws ParseException {
        if (incrementBy == 0)
            throw new ParseException(
                    String.format("Sequence %s has illegal value 0 for INCREMENT BY expression.", getName())
            );

        this.incrementBy = Optional.of(incrementBy);
    }

    void minValue(Integer minValue) {
        this.minValue = Optional.of(minValue);
    }

    void maxValue(Integer maxValue) {
        this.maxValue = Optional.of(maxValue);
    }

    void setIsCycle(Boolean isCycle) {
        this.isCycle = Optional.of(isCycle);
    }

    public Optional<Integer> getStartWith() {
        return startWith;
    }

    public Optional<Integer> getIncrementBy() {
        return incrementBy;
    }

    public Optional<Integer> getMinValue() {
        return minValue;
    }

    public Optional<Integer> getMaxValue() {
        return maxValue;
    }

    public Optional<Boolean> getIsCycle() {
        return isCycle;
    }

    @Override
    void save(BufferedWriter bw) throws IOException {
        Grain.writeCelestaDoc(this, bw);
        bw.write(String.format("CREATE SEQUENCE %s ", getName()));

        if (startWith.isPresent()) {
            bw.write(String.format("START WITH %s ", startWith.get()));
        }

        if (incrementBy.isPresent()) {
            bw.write(String.format("INCREMENT BY %s ", incrementBy.get()));
        }

        if (minValue.isPresent()) {
            bw.write(String.format("MINVALUE %s ", minValue.get()));
        }

        if (maxValue.isPresent()) {
            bw.write(String.format("MAXVALUE %s ", maxValue.get()));
        }

        if (isCycle.isPresent() && isCycle.get()) {
            bw.write("CYCLE ");
        }

        bw.write(";");
        bw.newLine();
        bw.newLine();
    }
}
