package ru.curs.celesta;

import ru.curs.celesta.score.Score;

public class CurrentScore {
    private static final ThreadLocal<Score> CURRENT = new ThreadLocal<>();

    public static Score get() {
        return CURRENT.get();
    }

    public static void set(Score score) {
        CURRENT.set(score);
    }
}
