package ru.curs.celesta.bad.annotated.twocallcontexts;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.annotation.CelestaProc;

public class TwoCallContexts {

    @CelestaProc
    public void twoCallContexts(CallContext callContext1, CallContext callContext2) {};
}
