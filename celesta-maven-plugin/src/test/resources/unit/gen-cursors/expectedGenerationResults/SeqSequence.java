package ru.curs.celesta.jcursor.score;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Sequence;

public final class SeqSequence extends Sequence {
    public SeqSequence(CallContext context) throws CelestaException {
        super(context);
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "seq";
    }
}
