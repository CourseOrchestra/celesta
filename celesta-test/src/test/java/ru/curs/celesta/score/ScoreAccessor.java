package ru.curs.celesta.score;

import ru.curs.celesta.CelestaException;

public final class ScoreAccessor {

    private ScoreAccessor() {
        throw new AssertionError();
    }

    public static Score createEmptyScore() {
        return new Score();
    }
}
