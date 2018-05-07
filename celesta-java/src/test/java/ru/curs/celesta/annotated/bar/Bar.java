package ru.curs.celesta.annotated.bar;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.annotation.CelestaProc;

public class Bar {

    @CelestaProc
    public String annotatedBarMethod(CallContext callContext, String suffix, int multiply) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < multiply; ++i) {
            sb.append(callContext.getSessionId())
                    .append(suffix);
        }

        return sb.toString();
    }

    public void notAnnotatedBarMethod() {
    }

}
