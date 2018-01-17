package ru.curs.celesta.score;

public final class ScoreAccessor {

    private ScoreAccessor() {
        throw new AssertionError();
    }

    public static Score createEmptyScore() {
        return new Score();
    }
}
