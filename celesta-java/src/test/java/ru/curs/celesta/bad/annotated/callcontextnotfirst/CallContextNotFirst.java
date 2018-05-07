package ru.curs.celesta.bad.annotated.callcontextnotfirst;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.annotation.CelestaProc;

public class CallContextNotFirst {

    @CelestaProc
    public void callContextNotFirst(int a, CallContext callContext) {}
}
