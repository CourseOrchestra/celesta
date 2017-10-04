package ru.curs.celesta;

public class CurrentCelesta {
    private static final ThreadLocal<Celesta> CURRENT = new ThreadLocal<>();

    public static Celesta get() {
        return CURRENT.get();
    }

    public static void set(Celesta celesta) {
        CURRENT.set(celesta);
    }
}
