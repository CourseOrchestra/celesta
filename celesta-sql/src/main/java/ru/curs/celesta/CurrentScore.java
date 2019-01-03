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

    /**
     * Returns score object. If global mode is on, single global score is returned.
     * Otherwise score object on a per thread basis is returned.
     */
    public static AbstractScore get() {
        if (globalMode) {
            return global;
        } else {
            return CURRENT.get();
        }
    }

    /**
     * Sets score object. If global mode is on, the score is saved as a global score.
     * Otherwise score object on a per thread basis is saved.
     *
     * @param score  Score object.
     */
    public static void set(AbstractScore score) {
        if (globalMode) {
            global = score;
        } else {
            CURRENT.set(score);
        }
    }

    /**
     * Sets if current score is in global mode.
     *
     * @param globalMode  {@code true} - global mode on, {@code false} - off.
     */
    public static void global(boolean globalMode) {
        CurrentScore.globalMode = globalMode;
    }
}
