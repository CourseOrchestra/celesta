package seq;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.dbutils.Sequence;

public final class SeqSequence extends Sequence {
    public SeqSequence(CallContext context) {
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
