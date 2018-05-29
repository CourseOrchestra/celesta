package ru.curs.celesta.java.bad.annotated.callcontextnotfirst;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.java.annotation.CelestaProc;

public class CallContextNotFirst {

    @CelestaProc
    public void callContextNotFirst(int a, CallContext callContext) {}
}
