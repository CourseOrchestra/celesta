package ru.curs.celesta;

import ru.curs.celesta.score.AbstractScore;

/**
 * ThreadLocal score accessor during the initialization of Celesta.
 */
public final class CurrentScore {
    private static final ThreadLocal<AbstractScore> CURRENT = new ThreadLocal<>();
    private static AbstractScore global = null;
    private static boolean globalMode;

    private CurrentScore() {
        throw new AssertionError();
    }

    public static AbstractScore get() {
        if (globalMode) {
            return global;
        }
        else {
            return CURRENT.get();
        }
    }

    public static void set(AbstractScore score) {
        if (globalMode) {
            global = score;
        }
        else {
            CURRENT.set(score);
        }
    }


    public static void global(boolean globalMode) {
        CurrentScore.globalMode = globalMode;
    }
}
