package ru.curs.celesta.callcontext;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.annotation.CelestaProc;

import java.util.function.Consumer;

public class CallContextInjection {

    @CelestaProc
    public Void run(CallContext callContext, Consumer<CallContext> contextConsumer) {
        contextConsumer.accept(callContext);
        return null;
    }
}
