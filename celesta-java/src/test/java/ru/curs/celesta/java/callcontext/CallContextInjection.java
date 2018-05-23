package ru.curs.celesta.java.callcontext;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.java.annotation.CelestaProc;

import java.util.function.Consumer;

public class CallContextInjection {

    @CelestaProc
    public Void run(CallContext callContext, Consumer<CallContext> contextConsumer) {
        contextConsumer.accept(callContext);
        return null;
    }
}
