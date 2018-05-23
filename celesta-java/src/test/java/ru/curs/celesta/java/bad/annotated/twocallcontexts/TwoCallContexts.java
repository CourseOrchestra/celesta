package ru.curs.celesta.java.bad.annotated.twocallcontexts;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.java.annotation.CelestaProc;

public class TwoCallContexts {

    @CelestaProc
    public void twoCallContexts(CallContext callContext1, CallContext callContext2) {};
}
