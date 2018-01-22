package ru.curs.celesta;

import ru.curs.celesta.score.AbstractScore;

public class CurrentScore {
    private static final ThreadLocal<AbstractScore> CURRENT = new ThreadLocal<>();

    public static AbstractScore get() {
        return CURRENT.get();
    }

    public static void set(AbstractScore score) {
        CURRENT.set(score);
    }
}
