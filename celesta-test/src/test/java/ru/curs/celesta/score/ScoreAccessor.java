package ru.curs.celesta.score;

public final class ScoreAccessor {

    private ScoreAccessor() {
        throw new AssertionError();
    }

    public static AbstractScore createEmptyScore() {
        return new AbstractScore();
    }
}
