package ru.curs.celesta.vintage;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.PyCelesta;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.concurrent.Future;
import java.util.function.Function;

public interface VintageCelesta extends PyCelesta {
    Object runProc(String sessionId, String qualifier, Object... args);
    Future<Object> runProcAsync(String sessionId, String qualifier, long delay, Object... args);
    Score getJavaScore();
    TriggerDispatcher getJavaTriggerDispatcher();
    CallContext getJavaCallContext();
    SessionContext getJavaSystemSessionContext();
    <T> T run(String sessionId, Function<CallContext, T> f);
    <T> Future<T> runAsync(String sessionId, Function<CallContext, T> f, long delay);
}
